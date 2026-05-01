CLINIC_NAME = "Hahnemann's Homoeo Pharmacy"
CLINIC_ADDRESS = "53 Boral Main Road, Garia"
CLINIC_CITY = "Kolkata 700084 - India"
CLINIC_PHONE = "+91 9830661016"


def clinic_profile_fields() -> dict:
    return {
        "clinic_name": CLINIC_NAME,
        "clinic_address": CLINIC_ADDRESS,
        "city": CLINIC_CITY,
        "clinic_phone": CLINIC_PHONE,
    }
