# Power Converter Data Extraction Pipeline

This serverless application extracts and processes power converter specifications from various manufacturers using Azure Functions.

## Architecture

The application uses Azure Durable Functions to orchestrate a multi-step extraction and processing pipeline:

1. **Scrape Series**: Extract product series information
2. **Scrape Products**: Extract detailed product information
3. **Download PDFs**: Retrieve and store product datasheets
4. **Extract PDF Data**: Extract text content from PDFs
5. **Extract Structured Data**: Parse structured data using AI
6. **Validate Data**: Validate and consolidate extracted data

## Triggering the Pipeline

The pipeline can be triggered in two ways:

### 1. Blob Storage Trigger (Recommended)

Upload a JSON configuration file to the `power-converter-data/config/` container to automatically trigger the pipeline:

```json
{
  "manufacturer": "recom",
  "product_types": ["dc-dc-converters", "ac-dc-power-supplies"]
}
```

This will trigger the `blob_trigger` function which starts the orchestrator.

### 2. HTTP Trigger (Manual)

Send a POST request to the `start_orchestrator` function with the following payload:

```json
{
  "manufacturer": "recom",
  "product_types": ["dc-dc-converters", "ac-dc-power-supplies"],
  "trigger_mode": "direct"
}
```

Setting `trigger_mode` to `blob` will create a blob that triggers the pipeline instead of starting it directly:

```json
{
  "manufacturer": "recom",
  "product_types": ["dc-dc-converters", "ac-dc-power-supplies"],
  "trigger_mode": "blob"
}
```

## Configuration

The application uses the following environment variables:

- `AzureWebJobsStorage`: Azure Storage connection string
- `STORAGE_CONTAINER`: Storage container name (default: `power-converter-data`)
- `DOCUMENT_INTELLIGENCE_ENDPOINT`: Azure Document Intelligence API endpoint
- `DOCUMENT_INTELLIGENCE_KEY`: Azure Document Intelligence API key
- `OPENAI_API_KEY`: OpenAI API key

## Supported Manufacturers

- RECOM Power
- Traco Power
- XP Power

## Outputs

The pipeline produces structured data in the following formats:

- CSV files for each step of the pipeline
- JSON files containing structured power converter specifications
- Content-addressable storage for PDFs and extracted text

All results are stored in the configured Azure Blob Storage container.