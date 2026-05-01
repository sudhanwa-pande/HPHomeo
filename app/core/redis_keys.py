def pending_login_key(temp_token: str) -> str:
    return f"pending_login:{temp_token}"


def otp_key(channel: str, identity: str, purpose: str) -> str:
    return f"otp:{channel}:{identity}:{purpose}"


def otp_attempts_key(channel: str, identity: str, purpose: str) -> str:
    return f"otp_attempts:{channel}:{identity}:{purpose}"


def otp_resend_lock_key(channel: str, identity: str, purpose: str) -> str:
    return f"otp_resend_lock:{channel}:{identity}:{purpose}"


def otp_resend_count_key(channel: str, identity: str, purpose: str) -> str:
    return f"otp_resend_count:{channel}:{identity}:{purpose}"


def blacklist_key(jti: str) -> str:
    return f"blacklist:{jti}"


def payment_order_key(appointment_id: str) -> str:
    return f"payment:order:{appointment_id}"


def payment_order_lock_key(appointment_id: str) -> str:
    return f"payment:order:lock:{appointment_id}"


def rate_key(ip: str, route: str) -> str:
    return f"rate:{ip}:{route}"
