import logging
import re
from email_validator import validate_email, EmailNotValidError
from disposable_email_domains import blocklist as disposable_domains

logger = logging.getLogger(__name__)

CUSTOM_BLOCKLIST = {
    "test.com", "test123.com", "example.com", "dummy.com", "fake.com",
    "test.in", "example.in", "xyz.com", "abc.com", "asdf.com"
}

def advanced_validate_email(value: str) -> dict:
    """
    Validates an email using a Hard Block vs Soft Score approach.
    Returns a dict with 'email' (normalized) and 'risk_score'.
    No heavy DNS queries are performed to ensure 0ms latency.
    """
    if not isinstance(value, str):
        raise ValueError("Email must be a string")

    try:
        # 1. Fast basic syntax check (no DNS deliverability check)
        valid = validate_email(value, check_deliverability=False)
        domain = valid.domain.lower()
        local_part = valid.local_part.lower()

        # 2. Hard Blocks (Immediate Reject)
        if domain in disposable_domains:
            raise ValueError("Disposable email addresses are not allowed")
        if domain in CUSTOM_BLOCKLIST:
            raise ValueError("Test or dummy email addresses are not allowed")
        if len(local_part) > 40:
            raise ValueError("Email prefix is too long")

        # 3. Soft Risk Scoring
        risk_score = 0

        # Low entropy garbage (e.g. asdfasdfasdf)
        if len(local_part) > 20 and len(set(local_part)) < 6:
            risk_score += 2
        
        # High digits
        if len(local_part) > 20 and sum(c.isdigit() for c in local_part) > 5:
            risk_score += 2

        return {
            "email": valid.normalized,
            "risk_score": risk_score
        }

    except EmailNotValidError as e:
        raise ValueError(f"Invalid email: {str(e)}")
