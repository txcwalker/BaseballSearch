# Databaseball

## Table of Contents
1. [Abstract](#abstract)
2. [Features](#features)
3. [Results](#results)
4. [Future Work](#future-work)
5. [Acknowledgments](#acknowledgments)
6. [Website](#website)

---

## Abstract
**Databaseball** is an interactive baseball statistics search engine that combines a **custom PostgreSQL database**, **daily-updating ETL pipelines**, and **natural language query translation**.  

The goal of the project is to make baseball data more accessible by allowing users to ask questions in plain English and receive accurate, data-driven answers backed by curated datasets.  

Key data sources include:  
- **Lahman Baseball Database** (historical stats)  
- **FanGraphs** (advanced batting and pitching statistics)  
- **Statcast** (daily pitch- and play-level data via `pybaseball`)  

---

## Features
- **Custom PostgreSQL Database**: Integrates historical, advanced, and live baseball data.  
- **Automated ETL Pipelines**: GitHub Actions + AWS RDS for daily FanGraphs and Statcast updates.  
- **Natural Language to SQL**: Uses LLMs to translate user questions into database queries.  
- **Interactive App**: Built with Streamlit for live querying and visualization.  
- **Data Normalization**: Unified schema ensures player stats are comparable across sources.  

---

## Results
- Successfully integrated Lahman, FanGraphs, and Statcast into one database.  
- Automated daily data updates with robust ETL pipelines.  
- Deployed an interactive web app where users can explore baseball data with **natural language queries**.  
- Produced visualizations and tables on-demand directly from the database.  

---

## Future Work
- Expand LLM prompt templates for more complex baseball queries.  
- Add team- and season-level summary dashboards.  
- Improve efficiency of daily ETL updates and scale database for larger Statcast coverage.  
- Extend the framework to additional sports datasets.  

---

## Acknowledgments
- **Lahman Baseball Database** for historical stats.  
- **FanGraphs** for advanced statistics.  
- **Statcast** via `pybaseball` for daily data.  
- Libraries: `pandas`, `sqlalchemy`, `scikit-learn`, `plotly`, `streamlit`.  

---

## Website
The Databaseball app is live here:  
👉 [Databaseball Streamlit App](https://databaseball.streamlit.app/)
