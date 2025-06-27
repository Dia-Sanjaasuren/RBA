# Streamlit App Deployment Guide

## Option 1: Streamlit Cloud (Recommended - Free & Easy)

### Step 1: Prepare Your Repository
1. Make sure your code is in a GitHub repository
2. Ensure you have a `requirements.txt` file (already created)
3. Make sure your main app file is named `home_2.py` (which it is)

### Step 2: Deploy to Streamlit Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository and branch
5. Set the main file path to: `home_2.py`
6. Click "Deploy"

### Step 3: Configure Environment Variables
For your Snowflake connection, you'll need to set environment variables:
1. In your Streamlit Cloud app settings
2. Go to "Secrets" section
3. Add your Snowflake credentials in this format:

```toml
[SNOWFLAKE_CONFIG]
user = "your_username"
password = "your_password"
account = "your_account"
warehouse = "your_warehouse"
database = "your_database"
schema = "your_schema"
```

### Step 4: Share Your App
- Your app will be available at: `https://your-app-name-your-username.streamlit.app`
- Share this URL with your users

## Option 2: Heroku (Alternative)

### Step 1: Create Procfile
Create a file named `Procfile` (no extension):
```
web: streamlit run home_2.py --server.port=$PORT --server.address=0.0.0.0
```

### Step 2: Create runtime.txt
Create a file named `runtime.txt`:
```
python-3.11.0
```

### Step 3: Deploy to Heroku
1. Install Heroku CLI
2. Run these commands:
```bash
heroku login
heroku create your-app-name
heroku config:set SNOWFLAKE_USER=your_username
heroku config:set SNOWFLAKE_PASSWORD=your_password
heroku config:set SNOWFLAKE_ACCOUNT=your_account
heroku config:set SNOWFLAKE_WAREHOUSE=your_warehouse
heroku config:set SNOWFLAKE_DATABASE=your_database
heroku config:set SNOWFLAKE_SCHEMA=your_schema
git add .
git commit -m "Deploy to Heroku"
git push heroku main
```

## Option 3: Railway (Modern Alternative)

### Step 1: Prepare for Railway
Create a `railway.json` file:
```json
{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "streamlit run home_2.py --server.port=$PORT --server.address=0.0.0.0",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
```

### Step 2: Deploy
1. Go to [railway.app](https://railway.app)
2. Connect your GitHub repository
3. Set environment variables for Snowflake
4. Deploy

## Option 4: Docker + Any Cloud Platform

### Step 1: Create Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "home_2.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Step 2: Deploy to Any Platform
- **Google Cloud Run**: `gcloud run deploy`
- **AWS ECS**: Use the Dockerfile
- **Azure Container Instances**: Use the Dockerfile
- **DigitalOcean App Platform**: Use the Dockerfile

## Security Considerations

### For Production Deployment:
1. **Environment Variables**: Never hardcode credentials
2. **HTTPS**: Ensure your deployment platform provides HTTPS
3. **Authentication**: Consider adding Streamlit authentication
4. **Rate Limiting**: Implement if needed for your use case

### Add Authentication (Optional)
Create a `.streamlit/config.toml` file:
```toml
[server]
enableCORS = false
enableXsrfProtection = true
```

## Troubleshooting

### Common Issues:
1. **Import Errors**: Make sure all dependencies are in `requirements.txt`
2. **Connection Issues**: Check your Snowflake credentials and network access
3. **Memory Issues**: Consider upgrading your deployment plan if needed

### Performance Tips:
1. Use `@st.cache_data` and `@st.cache_resource` (already implemented)
2. Optimize database queries
3. Consider pagination for large datasets

## Recommended Approach

**For your use case, I recommend Streamlit Cloud because:**
- ✅ Free tier available
- ✅ Easy deployment
- ✅ Automatic HTTPS
- ✅ Good performance
- ✅ Built specifically for Streamlit apps
- ✅ Easy environment variable management

## Next Steps

1. **Choose your deployment platform** (Streamlit Cloud recommended)
2. **Set up your GitHub repository** if not already done
3. **Configure environment variables** for Snowflake
4. **Deploy and test**
5. **Share the URL** with your users

Your app will be accessible via a public URL that you can share with anyone! 