# Project Overview & Architecture Guide

**Target Audience**: Data Delivery Managers, Stakeholders, and Engineering Leads.  
**Purpose**: To explain **what** we have built, **why** it is designed this way, and **how** it delivers reliable financial data.

---

## 1. Executive Summary

We have built a **Modern ELT (Extract, Load, Transform) Platform**.

In simple terms:
1.  **Extract**: We fetch raw financial data from external vendors (like Massive, Yahoo, Polygon).
2.  **Load**: We save this data *immediately* into a secure vault (our Data Warehouse) in its original form.
3.  **Transform**: We use automated recipes to clean, combine, and check this data to create "Trusted Tables" for analysts.

**Key Benefit**: By separating "saving data" from "cleaning data", we ensure that **we never lose source data**. If our cleaning formulas change, we simply re-run the recipes on the raw data we already have.

---

## 2. The Data Flow (Pipeline)

Imagine a manufacturing assembly line.

### **Phase 1: Ingestion (The Delivery Trucks)**
*   **What happens**: Python scripts wake up on a schedule (e.g., 8:00 AM) and call external APIs to ask for the latest stock prices.
*   **Destination**: The `raw` schema in our database.
*   **Business Value**: This layer is dumb but robust. Its only job is to get data into the building safely. It handles API failures, rate waits, and retries automatically.

### **Phase 2: Storage (The Warehouse)**
*   **Where**: DigitalOcean Managed PostgreSQL.
*   **Why**: This is a production-grade database that is backed up automatically. It is separate from the "Application Server", so even if the application crashes or is deleted, the **data remains safe**.
    *   **Schema `raw`**: The "Landing Zone". Messy, duplicate-prone, exactly what the vendor sent.
    *   **Schema `cdm` (Common Data Model)**: The "Showroom". Clean, deduped, standardized tables ready for Tableau/BI.

### **Phase 3: Transformation (The Quality Control)**
*   **Tool**: `dbt` (Data Build Tool).
*   **What happens**: After ingestion finishes, `dbt` runs a series of SQL models.
    *   *Example*: "Take raw prices, remove weekends, convert currency, calculate 30-day moving average."
*   **Testing**: We run automated quality checks (e.g., "Stop if Apple's stock price is negative"). Bad data is flagged *before* it reaches the dashboard.

### **Phase 4: Orchestration (The Site Manager)**
*   **Tool**: Apache Airflow.
*   **Role**: The conductor that coordinates everything.
*   **Why**: It ensures tasks happen in the right order. It won't try to "Clean Data" until "Fetch Data" has successfully finished. If a step fails, it alerts the team immediately.

---

## 3. Environment Strategy

To prevent "it works on my machine" issues, we use a strict two-tier environment.

| Feature | Local Environment (Your Laptop) | Production Environment (The Server) |
| :--- | :--- | :--- |
| **Purpose** | Experimentation, Development, Breaking things. | Stability, Reliability, "Source of Truth". |
| **Data Scope** | Small, fake, or subset of real data. | Full historical dataset (Massive). |
| **Database** | A disposable storage container (Docker). | A permanent, backed-up Cloud Database. |
| **Cost** | Free. | Paid (DigitalOcean Droplet + DB). |

**The Golden Rule**: We never manually change things on the Production Server. We make changes locally, test them, and then "Deploy" them.

---

## 4. Deployment & Security

### **How Code Travels**
1.  **Developer** writes code on their Mac.
2.  **Developer** pushes code to GitHub (Version Control).
3.  **Server** pulls the code from GitHub.
4.  **Server** updates its instructions and begins running the new logic.

### **Security Measures**
*   **Credentials**: Passwords are never saved in the code. They are injected via "Environment Variables" on the secure server.
*   **Network**: The Database only accepts connections from our specific server and authorized developer IPs (whitelisted).
*   **Audit**: Every change to the code is tracked in Git with a timestamp and author.

---

## 5. Frequently Asked Questions (FAQ)

**Q: Why do we use Python AND SQL?**
A: Python is the best tool for talking to the internet (APIs). SQL is the best tool for crunching numbers inside a database. We use the right tool for the right job.

**Q: What happens if the API is down?**
A: Airflow will retry extraction several times. If it still fails, it sends an alert, and the "Transformation" step is paused so we don't accidentally report incomplete data.

**Q: Can I connect Excel/Tableau directly?**
A: Yes. You should connect to the **`cdm`** or **`analysis`** schemas. Do not connect to `raw`—it serves as the implementation detail, not the user interface.

**Q: How do we fix a data error?**
A: We fix the "Recipe" (SQL logic) in `dbt`, deploy the change, and re-run. The system effectively "rewrites history" using the correct logic on the original source data.

