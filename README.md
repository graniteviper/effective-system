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

### 2. Frontend Setup (Next.js)

```bash
cd full-stack
npm install
npm run dev
```

Frontend will run at `http://localhost:3000`.

---

## 🧪 How to Use

### Postgres via docker
1. Make sure you have docker installed and docker engine running.
2. Run the command `docker run --name connector -e POSTGRES_PASSWORD=supersecretpassword -p 5432:5432 -d postgres`.

### 🔧 GitHub Extractor

1. Select **GitHub** from the dropdown.
2. Enter your GitHub Personal Access Token (Make sure the token has the required permissions).
3. Click **Fetch Repositories**.
4. For any repository:

   * Click **See Issues** to view issues.
   * Click **Save Issues to DB** to store them in PostgreSQL.
   * If no issues are found, a message is shown.

### 🧠 Notion Extractor

1. Select **Notion** from the dropdown.
2. Enter your Notion Integration Token (Make sure the token has the required files accessible).
3. Click **Show Databases**.
4. For each database:

   * Click **Save to DB** to extract and store entries.
   * Click **Show Data** to view previously stored entries.
   * If no entries are found, a message is shown.

---

## 🛠️ Technologies Used

* **Frontend**: Next.js 14, React, Tailwind CSS
* **Backend**: Python, FastAPI
* **Storage**: PostgreSQL
* **APIs**:

  * GitHub REST API
  * Notion SDK & API

---

## 📌 Notes

* Ensure GitHub PAT has `repo` scope.
* Ensure Notion Integration is added to each workspace/database you're accessing.
* All extracted data is stored in Postgres under relevant tables like `github_issues` and `notion_data`.

---

## ✅ Future Improvements

* Schema-aware dynamic table creation.
* Add other connectors (Google Sheets, Jira, etc.).
* UI filtering and pagination.
* Addin OAuth as the next upgrade.

