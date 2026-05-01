# Test Checklist (Pre-Phase 10/11)

Base URL:

```bash
export BASE_URL="http://localhost:8000/api/v1"
```

## 1. Doctor Register + Phone Validation

### 1.1 Register doctor (valid phone) -> `200`

```bash
curl -s -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "full_name":"Dr Test One",
    "email":"dr1@example.com",
    "phone":"+919876543210",
    "password":"Strong@123",
    "registration_no":"REG-001"
  }'
```

Expected:
- `200`
- response has `access_token`, `refresh_token`, `doctor_id`

### 1.2 Register doctor without phone -> `422`

```bash
curl -s -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "full_name":"Dr Missing Phone",
    "email":"dr2@example.com",
    "password":"Strong@123",
    "registration_no":"REG-002"
  }'
```

Expected:
- `422` validation error for missing `phone`

### 1.3 Register doctor with duplicate phone -> `409`

```bash
curl -s -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "full_name":"Dr Duplicate Phone",
    "email":"dr3@example.com",
    "phone":"+919876543210",
    "password":"Strong@123",
    "registration_no":"REG-003"
  }'
```

Expected:
- `409`
- detail contains `Phone already registered` (or duplicate conflict message)

### 1.4 `/auth/me` returns phone

Use the access token from step 1.1.

```bash
export DOCTOR_TOKEN="<paste_access_token>"
curl -s "$BASE_URL/auth/me" -H "Authorization: Bearer $DOCTOR_TOKEN"
```

Expected:
- `200`
- includes `"phone": "+919876543210"`

### 1.5 `GET /doctor/profile` returns phone

```bash
curl -s "$BASE_URL/doctor/profile" -H "Authorization: Bearer $DOCTOR_TOKEN"
```

Expected:
- `200`
- includes `"phone": "+919876543210"`

## 2. Index Creation Sanity

Run once against Mongo shell:

```javascript
db.doctors.getIndexes()
```

Expected:
- unique index on `email`
- unique index on `phone`
- unique index on `registration_no`

## 3. Patient OTP Baseline (Current Flow)

### 3.1 Request OTP -> `200`

```bash
curl -s -X POST "$BASE_URL/patient/auth/request-otp" \
  -H "Content-Type: application/json" \
  -d '{
    "phone":"+919999999999",
    "purpose":"login"
  }'
```

Expected:
- `200`
- `{"message":"otp_sent"}`
- OTP appears in local terminal logs in current implementation

### 3.2 Verify OTP (correct) -> `200`

```bash
curl -s -X POST "$BASE_URL/patient/auth/verify-otp" \
  -H "Content-Type: application/json" \
  -d '{
    "phone":"+919999999999",
    "purpose":"login",
    "code":"<otp_from_terminal>"
  }'
```

Expected:
- `200`
- response has patient access/refresh tokens

### 3.3 Verify OTP (wrong) -> `401`

```bash
curl -s -X POST "$BASE_URL/patient/auth/verify-otp" \
  -H "Content-Type: application/json" \
  -d '{
    "phone":"+919999999999",
    "purpose":"login",
    "code":"000000"
  }'
```

Expected:
- `401`
- `Invalid or expired OTP`

## 4. Request ID Header Check

```bash
curl -i -s "$BASE_URL/health"
```

Expected:
- response header includes `X-Request-ID`

## 5. Ready-to-Start Gate

Proceed to Phase 10 only when all are true:
- doctor phone validation/conflict checks pass
- doctor phone visible in `/auth/me` and `/doctor/profile`
- doctor unique phone index exists
- patient OTP baseline still works
- `X-Request-ID` present in responses

