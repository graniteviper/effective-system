

# Data Connector API Assignment

## Overview

This repository provides a minimal backend and API scaffold for building data connectors between business applications (sources) and storage targets (S3 or Postgres).  
You are provided with:
- An example source connector (Salesforce)
- An example target (S3)
- Base classes and conventions for connectors and storage
- A minimal Next.js API route example

Your task is to implement additional connectors and expose them via API endpoints, following the provided conventions.

---

## Getting Started

### 1. **Clone the Repository**

```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

### 2. **Set Up Environment Variables**

Copy the example environment file and fill in your credentials:

```bash
cp .env.local.example .env.local
```

Edit `.env.local` and provide values for:
- Supabase credentials
- Salesforce credentials (if using)
- S3 or Postgres credentials (depending on your chosen target)
- Any other connector credentials you need

### 3. **Install Dependencies**

#### **Backend (Python)**
```bash
cd backend
pip install -r requirements.txt
```

#### **Frontend/API (Node.js/Next.js)**
```bash
npm install
```

### 4. **Run the Backend**

If your backend is a standalone service, start it (adjust as needed):

```bash
# Example: if using FastAPI or Flask
python -m backend.extractors
```

### 5. **Run the Next.js App**

```bash
npm run dev
```

---

## Project Structure

```
backend/
  extractors/
    connectors/           # Connector implementations (Salesforce example provided)
    base/                 # Base classes for connectors and storage
    storage/              # Storage targets (S3 example provided)
    extractors/           # Extraction orchestration
  requirements.txt

app/
  api/                    # Next.js API routes (example provided)
.env.local.example        # Environment variable template
README.md                 # This file
```

---

## What’s Provided

- **Example source connector:** `salesforce_connector.py`
- **Example target:** `s3_storage.py`
- **Base classes:** `api_connector.py`, `base_storage_manager.py`, `base_extractor.py`
- **Minimal API route:** `app/api/extractors/[source]/objects/route.ts`

---

## Your Tasks

1. **Implement two new source connectors** (not Google Sheets or HubSpot).
2. **Implement one new target** (S3 or Postgres).
3. **Expose connector functionality as RESTful API endpoints.**
4. **Build a minimal Next.js UI** to demonstrate connecting, extracting, and loading data.
5. **Update this README** with setup and usage instructions for your implementation.

---

## Tips

- Follow the code structure and conventions in the provided files.
- Use environment variables for all credentials and secrets.
- Handle errors and edge cases gracefully.
- The UI can be minimal but should demonstrate the end-to-end flow.

---

## License

This project is for assignment and demonstration purposes.

---

**Good luck!**

