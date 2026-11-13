"""Verify SNS HTTP signatures BEFORE subscription confirmation or alert delivery.

AWS documentation: sns-verify-signature-of-message-verify-message-signature.html
The SigningCertURL must be pinned to the expected AWS SNS regional hostname.
"""
import base64
import json
from datetime import datetime, timezone
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


class SNSVerificationError(ValueError):
    pass


def _check_url(url, region, cert=True):
    parsed = urlsplit(url)
    expected = f"sns.{region}.amazonaws.com"
    if (parsed.scheme != "https" or parsed.hostname != expected or parsed.port is not None
            or parsed.username or parsed.password or parsed.fragment):
        raise SNSVerificationError("Untrusted SNS URL")
    if cert and (not parsed.path.endswith(".pem") or not parsed.path.startswith("/SimpleNotificationService-")):
        raise SNSVerificationError("Unexpected SNS signing certificate path")
    return url


def canonical_string(payload):
    typ = payload.get("Type")
    if typ == "Notification":
        keys = ["Message", "MessageId", "Subject", "Timestamp", "TopicArn", "Type"]
    elif typ in ("SubscriptionConfirmation", "UnsubscribeConfirmation"):
        keys = ["Message", "MessageId", "SubscribeURL", "Timestamp", "Token", "TopicArn", "Type"]
    else:
        raise SNSVerificationError("Unknown SNS message type")
    if "Subject" not in payload and "Subject" in keys:
        keys.remove("Subject")
    try:
        return "".join(f"{key}\n{payload[key]}\n" for key in keys).encode("utf-8")
    except (KeyError, TypeError) as ex:
        raise SNSVerificationError(f"Missing SNS signature field: {ex}") from ex


def verify_sns(payload: dict, *, expected_topic_arn: str, region: str, fetch_certificate=None,
               now=None, max_age_s=86400):
    if not expected_topic_arn:
        raise SNSVerificationError("SNS_TOPIC_ARN is not configured")
    if not isinstance(payload, dict) or payload.get("TopicArn") != expected_topic_arn:
        raise SNSVerificationError("Unexpected SNS topic")
    if payload.get("SignatureVersion") not in ("1", "2"):
        raise SNSVerificationError("Unsupported SNS signature version")
    # Guard against signed stale notifications or replay outside the deduplication window
    try:
        received = datetime.fromisoformat(payload["Timestamp"].replace("Z", "+00:00")).timestamp()
    except (KeyError, ValueError, TypeError) as ex:
        raise SNSVerificationError("Bad SNS timestamp") from ex
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    if abs(now - received) > max_age_s:
        raise SNSVerificationError("SNS timestamp outside permitted range")
    url = _check_url(payload.get("SigningCertURL", ""), region)
    if payload["Type"] == "SubscriptionConfirmation":
        _check_url(payload.get("SubscribeURL", ""), region, cert=False)
    body = canonical_string(payload)
    fetch_certificate = fetch_certificate or _download_cert
    certificate = x509.load_pem_x509_certificate(fetch_certificate(url))
    if not certificate.not_valid_before_utc.timestamp() <= now <= certificate.not_valid_after_utc.timestamp():
        raise SNSVerificationError("Expired SNS signing certificate")
    key = certificate.public_key()
    if not isinstance(key, rsa.RSAPublicKey):
        raise SNSVerificationError("SNS signing key must be RSA")
    try:
        key.verify(base64.b64decode(payload["Signature"], validate=True), body, padding.PKCS1v15(),
                   hashes.SHA256() if payload["SignatureVersion"] == "2" else hashes.SHA1())
    except Exception as ex:
        raise SNSVerificationError("Invalid SNS signature") from ex
    return payload["Type"]


def _download_cert(url):
    # Reject redirects BEFORE they are followed to avoid fetching untrusted hosts.
    class RejectRedirects(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            raise SNSVerificationError("Redirect from SNS certificate URL")

    with build_opener(RejectRedirects()).open(url, timeout=5) as response:
        cert = response.read(64 * 1024 + 1)
        if len(cert) > 64 * 1024:
            raise SNSVerificationError("SNS certificate too large")
        return cert
