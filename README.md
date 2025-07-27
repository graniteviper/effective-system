Here’s a `README.md` for your full-stack data extraction tool using GitHub and Notion. It includes a description, features, setup steps, and usage instructions:

---

### Video Link: https://drive.google.com/file/d/1cOcLxjlwjcotJvDl3jrZ5zyXabCyVC_O/view?usp=drive_link
### Repo Link: https://github.com/graniteviper/effective-system

```markdown
# 📊 Full-Stack Data Extractor – GitHub & Notion Integration

This project is a full-stack application that lets you extract data from GitHub repositories (issues) and Notion databases, and save it into a centralized PostgreSQL database. It features a clean UI for authentication, data preview, and persistence.

## 🌟 Features

- 🔐 Authenticate with GitHub (Personal Access Token) and Notion (Integration Token)
- 📦 Extract GitHub repositories and their issues
- 📚 Extract Notion databases and their entries
- 💾 Save extracted data into a PostgreSQL database
- 👀 View stored data in the frontend
- ⚡ Built with Next.js (frontend + API routes) and Python backend (FastAPI or Flask-compatible extract/load logic)

---

## 🗂️ Project Structure

```

assignment/

├── backend/

│   └── extractors/

│       ├── connectors/

│       │   ├── github.py

│       │   └── notion.py

│       └── storage/

│           └── postgres.py

├── full-stack/

│   └── (Next.js frontend + API routes)

└── README.md

````

---

## 🚀 Getting Started

### 1. Backend Setup (Python)
- Create and activate a virtual environment:

```bash
cd backend
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
````

* Install dependencies:

```bash
cd backend
pip install -r requirements.txt
```

### For zoho:
```
python -m backend.extractors.tests.test_zoho_extractor
```


### For Sugarcrm:
```
python -m backend.extractors.tests.test_sugarcrm_extractor
```