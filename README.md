# 🎬 Film Data Warehouse & OLAP Analytics

This project is an end-to-end **Business Intelligence & Analytics solution** built around a **Film Data Warehouse**, combining ETL processing, OLAP cube modeling, and interactive visual analytics using **Atoti**.

The goal is to analyze movie performance from multiple perspectives such as time, genre, studio, country, and financial indicators.

---

##  Project Overview

- Designed a **star schema Data Warehouse** for film analytics
- Built a complete **ETL pipeline** (Extract, Transform, Load)
- Created **6 OLAP cubes** for multidimensional analysis
- Developed an **interactive Atoti dashboard** for real-time exploration and visualization

---

##  Architecture

**Data Source**
- Excel dataset containing film metadata and performance metrics

**ETL Layer**
- Python (Pandas, NumPy)
- Data cleaning & transformation
- Derived metrics (Profit, ROI)
- Time dimension extraction (Year, Quarter, Month)

**Storage**
- PostgreSQL Data Warehouse
- Fact & Dimension tables (Star Schema)

**Analytics & Visualization**
- Atoti OLAP session
- Interactive dashboard with drill-down & slicing

---

##  Data Warehouse Schema

### Fact Table
- `FactFilmPerformance`
  - Budget
  - Box Office
  - Profit
  - ROI
  - Oscar Wins & Nominations
  - Runtime

### Dimensions
- `DimFilm`
- `DimTime`
- `DimDirector`
- `DimStudio`
- `DimGenre`
- `DimCountry`
- `DimLanguage`

---

##  OLAP Cubes (6 Cubes)

The project includes **6 OLAP cubes**, enabling analysis such as:

- Financial performance over time
- ROI comparison by genre and studio
- Movie success by country and language
- Awards impact on box office performance
- Runtime vs profitability analysis
- Trend analysis with drill-down capabilities

All cubes are visualized through an **interactive Atoti dashboard**.

---

##  Features

- Automated ETL process
- Clean and scalable data model
- Advanced OLAP analytics
- Interactive dashboard (filtering, slicing, drill-down)
- Business-oriented KPIs

---

##  Tech Stack

- **Python** (Pandas, NumPy)
- **PostgreSQL**
- **Atoti**
- **OLAP / Multidimensional Modeling**
- **Business Intelligence**

---

## How to Run

1. Configure PostgreSQL connection in the ETL script
2. Place the Excel dataset in the correct path
3. Run the ETL script:
   ```bash
   python etl_film_datawarehouse.py
