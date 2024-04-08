from flask import Flask, request, jsonify
from flask_cors import CORS 
from rag import data, service, logger, util
import concurrent.futures
import tempfile
import pandas as pd
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
logger.setup_logger()
executor = concurrent.futures.ProcessPoolExecutor(max_workers=1)


@app.route('/company/batch', methods=['POST'])
def batch_mode():
    # Get the JSON data from the request
    request_data = request.get_json()
    response = get_response(request_data)
    return jsonify({"response":response})


@app.route('/company/live', methods=['POST'])
def live_mode():
    request_data = request.get_json()
    response = get_response(request_data)
    return jsonify({"response":response})


@app.route('/company/query', methods=['POST'])
def query():
    # Get the JSON data from the request
    request_data = request.get_json()
    company_name = request_data['company']
    company_url = request_data['url']
    response = service.fetch_vc_information(company_name, company_url, util.generate_id())
    return jsonify(response)


@app.route('/result/<int:request_id>')
def result(request_id):
    return jsonify({"results":[request_id]})


@app.route('/upload', methods=['POST'])
def upload_pdf():
    try:
        # Check if the POST request contains a file
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            return jsonify({'error': 'No selected file'})

        # Check if the file is a PDF
        if file.filename.endswith('.xlsx'):
            return upload_excel(file)
        elif not file.filename.endswith('.pdf'):
            return jsonify({'error': 'Invalid file type. Please upload a PDF file'})

        # Create a temporary file to store the PDF content
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
            temp_pdf.write(file.read())
            temp_pdf_path = temp_pdf.name

        name_url_dict = service.extract_http_links_from_pdf(temp_pdf_path)

        # Remove the temporary file
        temp_pdf.close()

        return jsonify(name_url_dict)
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'})
        
def get_response(request_data):
    company_name = str(request_data['company']).lower().replace(" ", "")
    structured_data = data.get_structured_data_by_company(company_name)
    unstructured_data = data.get_unstructured_data_by_company(company_name)
    return service.conflate(company_name, structured_data, unstructured_data)
        
def upload_excel(file):
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_xl:
            temp_xl.write(file.read())
            temp_xl_path = temp_xl.name
        df = pd.read_excel(temp_xl_path)
        df.rename(lambda x: " ".join(x.lower().split()), axis="columns", inplace=True)
        df.rename(lambda x: x.strip(), axis="columns", inplace=True)
        df['contact'] = ''
        
        if not validate_excel_data(df):
            return jsonify({'error': f'Excel File validation failed'})

        df.dropna(subset=["company name", "url"], inplace=True)
        colsToKeep = ['company name', 'url', 'description(full)', 'location', 'founded year',  'total funding', 'industry', 'contact']
        df = df[colsToKeep]
        json_data = df.rename(columns={"company name": "company", "description(full)" : "description", "founded year": "year"}).to_json(orient="records")
        
        temp_xl.close()
        # return jsonify(json_data)
        #return json_data
        return app.response_class(response=json.dumps(json_data), status=200, mimetype='application/json')

    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'})

    
def validate_excel_data(df):
    missing_fields = []
    # Check if "Company Name" and "URL" columns are present
    if "company name" not in df.columns:
        missing_fields.append("'company name'")
    if "url" not in df.columns:
        missing_fields.append("'url'")

    # Check for empty values in "Company Name" and "URL" columns
    missing_values = df[df["company name"].isnull() | df["url"].isnull()]
    if not missing_values.empty:
        missing_fields.extend(missing_values.index.tolist())

    if missing_fields:
        #logging.error(f"Missing fields or values: {', '.join(missing_fields)}")
        return False
    return True

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8080', debug=True)