# Strategic Go-to-Market (GTM) Agent 
The Strategic GTM Agent is an AI-powered application designed to automate and enhance prospect analysis for sales and go-to-market teams. It integrates internal company data from Google BigQuery with the analytical power of Google's Gemini model to deliver rich, actionable strategic insights in seconds. 
## Features 
- **Individual Analysis:** Generate a deep-dive report on a single company. 
- **Batch Analysis:** Upload a CSV/Excel file to analyze hundreds of companies at once. 
- **Data-Driven Insights:** Analysis is enriched with internal data on customers, products, and sales plays. 
- **AI-Generated Content:** The agent generates key messages, suggests outreach channels, and estimates value propositions. 
- **Data Explorer:** A built-in UI to browse and query the underlying BigQuery tables. 
- **Secure Authentication:** User access is managed via Google OAuth 2.0. 
## Architecture 
The application uses a three-tier architecture running on Google Cloud: 
1.  **Frontend:** A React/Tailwind CSS single-page application. 
2.  **Backend:** A Python/Flask agent that orchestrates data retrieval and AI analysis. 
3.  **Data & AI:** Google BigQuery for data warehousing and Google Vertex AI (Gemini) for analysis. 
## Setup and Deployment 
### Prerequisites 
- A Google Cloud Project with Billing enabled. 
- The `gcloud` CLI installed and authenticated.
- APIs enabled: Cloud Run, Cloud Build, Vertex AI, BigQuery. 
### Environment Variables 
The application requires the following environment variables to be set in the Cloud Run service configuration: 
- `GCP_PROJECT_ID`: Your Google Cloud Project ID. 
- `DATASET_ID`: The BigQuery dataset ID (e.g., `ca_hk_team6_ds`). 
- `OAUTH_CLIENT_ID`: The Client ID for your Google OAuth 2.0 credential. 
- `FLASK_SECRET_KEY`: A secret key for Flask sessions (can be generated with `os.urandom(24)`). 
### Deployment 

Deploy the application to Google Cloud Run using the gcloud CLI from the project's root directory: 

```bash 
gcloud run services proxy cahkteam6gtmapp --region=us-central1
 ```
