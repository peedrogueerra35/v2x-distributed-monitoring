"""SNS verification tests use local ephemeral keys: no network or AWS required."""
import base64
from datetime import datetime, timedelta, timezone

import pytest
pytest.importorskip('cryptography')
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.x509.oid import NameOID

from services.rsu.sns_security import SNSVerificationError, canonical_string, verify_sns


def signed_notice():
    t = datetime.now(timezone.utc)
    key=rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'sns.us-east-1.amazonaws.com')])
    cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(t-timedelta(days=1))
          .not_valid_after(t+timedelta(days=1)).sign(key,hashes.SHA256()))
    pem=cert.public_bytes(serialization.Encoding.PEM)
    payload={"Type":"Notification","Message":"{\"alert_id\":\"a\"}","MessageId":"message-1",
             "Timestamp":t.isoformat(),"TopicArn":"arn:aws:sns:us-east-1:123456789012:v2x-alerts",
             "SigningCertURL":"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-1234.pem",
             "SignatureVersion":"2"}
    sig=key.sign(canonical_string(payload),padding.PKCS1v15(),hashes.SHA256())
    payload['Signature']=base64.b64encode(sig).decode('ascii')
    return payload,pem


def check(payload,pem):
    return verify_sns(payload,expected_topic_arn='arn:aws:sns:us-east-1:123456789012:v2x-alerts',
                      region='us-east-1',fetch_certificate=lambda _:pem)


def test_notification_signature_valid():
    payload,pem=signed_notice()
    assert check(payload,pem)=='Notification'


def test_tampered_body_rejected():
    payload,pem=signed_notice()
    payload['Message']='dangerous change'
    with pytest.raises(SNSVerificationError): check(payload,pem)


def test_certificate_ssrf_rejected_before_fetching():
    payload,pem=signed_notice()
    payload['SigningCertURL']='https://attacker.example/evil.pem'
    with pytest.raises(SNSVerificationError): check(payload,pem)


def test_wrong_topic_rejected():
    payload,pem=signed_notice()
    payload['TopicArn']='arn:aws:sns:us-east-1:123456789012:other'
    with pytest.raises(SNSVerificationError): check(payload,pem)
