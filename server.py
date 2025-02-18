
import logging
from logging.handlers import RotatingFileHandler

import cgi
import io
import json
import pandas as pd
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from app.az import *
from app.utils import get_last_value, get_date_file_with_type


if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('ServerLogger')
logger.setLevel(logging.INFO)

log_file = os.path.join('logs', 'server.log')
handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
logger.addHandler(handler)


class RouterHandler(BaseHTTPRequestHandler):

    get_routes = {
        '/experiments': 'handle_home'
    }

    post_routes = {
        '/experiments/api/az/upload/sponsored_brands': 'handle_az_sb_upload',
        '/experiments/api/az/upload/sponsored_products': 'handle_az_sp_upload',
        '/experiments/api/az/upload/sponsored_display': 'handle_az_sd_upload',
        '/experiments/api/az/upload/business_report': 'handle_az_br_upload',
        '/experiments/api/az/upload/az_map': 'handle_az_map_upload',
        '/experiments/api/az/upload/campaign_map': 'handle_az_campaign_map_upload',
        '/experiments/api/az/request_dashboard_metrics': 'handle_dashboard_metrics'
    }

    routes = {**get_routes, **post_routes}

    def _log_request_info(self):
        """Log information about the incoming request"""
        client_address = self.client_address[0]
        request_line = self.requestline
        logger.info(f"Request from {client_address}: {request_line}")

    def _log_error_info(self, error_msg):
        """Log error information"""
        client_address = self.client_address[0]
        logger.error(f"Error for {client_address}: {error_msg}")

    def do_GET(self):
        self._log_request_info()
        # Parse the URL
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        # Check if route exists
        if path in self.get_routes:
            # Get the handler method name and call it
            handler_name = self.routes[path]
            handler = getattr(self, handler_name, self.handle_404)
            handler(query_params)
        else:
            self.handle_404()

    def do_POST(self):
        self._log_request_info()
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        if path in self.post_routes:
            handler_name = self.routes[path]
            handler = getattr(self, handler_name, self.handle_404)
            handler(query_params)
        else:
            self.handle_404()

    def send_response_content(self, content, status=200, content_type='text/html'):
        self.send_response(status)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(bytes(content, "utf8"))

        # Log response
        logger.info(f"Response sent: Status {status}, Content-Type: {content_type}")

    # Route handlers
    def handle_home(self, params):
        content = """
        <h1>Welcome to the Home Page</h1>
        <p>Try visiting /about or /api/users</p>
        """
        self.send_response_content(content)

    def _get_df_from_upload(self, params):
        try:
            # Parse the multipart form data
            content_type = self.headers.get('Content-Type')

            if not content_type or not content_type.startswith('multipart/form-data'):
                raise ValueError("Invalid content type. Must be multipart/form-data")

            # Parse the form data
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={
                    'REQUEST_METHOD': 'POST',
                    'CONTENT_TYPE': self.headers['Content-Type'],
                }
            )

            # Get the uploaded file
            file_item = form['file']
            action = form.getvalue('action', 'preview')

            # Read the file content
            file_content = file_item.file.read()
            file_extension = os.path.splitext(file_item.filename)[1].lower()
            client_name = form.getvalue("client_name")

            # Process the file based on its type
            if file_extension == '.csv':
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                raise ValueError(f"Unknown file")

            logger.info(f"Processing file: {file_item.filename}, Client: {client_name}")
            logger.info(f"Date range: {start_date} to {end_date}")
            return df, client_name
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sb_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = upload_sb_data(calculated_df, client_name)

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_map_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = upload_asin_cat_map(calculated_df, client_name)

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_campaign_map_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = upload_campaign_cat_map(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sp_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = upload_sp_data(calculated_df, client_name)

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sd_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = upload_sd_data(calculated_df, client_name)

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_br_upload(self, params):
        try:
            calculated_df, client_name = self._get_df_from_upload(params)

            response_data = []

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_dashboard_metrics(self, params):
        try:
            # Parse the multipart form data
            content_type = self.headers.get('Content-Type')

            if not content_type or not content_type.startswith('multipart/form-data'):
                error_msg = "Invalid content type. Must be multipart/form-data"
                self.log_error_info(error_msg)
                raise ValueError("Invalid content type. Must be multipart/form-data")

            # Parse the form data
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={
                    'REQUEST_METHOD': 'POST',
                    'CONTENT_TYPE': self.headers['Content-Type'],
                }
            )

            # Get the uploaded file
            file_item = form['file']

            # Read the file content
            file_content = file_item.file.read()
            file_extension = os.path.splitext(file_item.filename)[1].lower()

            # Process the file based on its type
            if file_extension == '.csv':
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                raise ValueError(f"Unknown file")

            start_date = form.getvalue("start_date")
            end_date = form.getvalue("end_date")
            client_name = form.getvalue("client_name")
            category_list = form.getvalue("category_list")

            modified_category_list = category_list.split(",")
            response_data = calculate_complete_category_metrics(
                df, client_name, start_date, end_date, modified_category_list)

            logger.info(f"Processed file: {file_item.filename}, Client: {client_name}, Date range: {start_date} to {end_date}, category list: {category_list}")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_msg = str(e)
            self.log_error_info(error_msg)
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_404(self, params=None):
        error_msg = f"404 Not Found: {self.path}"
        logger.warning(error_msg)
        content = "<h1>404 Not Found</h1><p>The requested route does not exist.</p>"
        self.send_response_content(content, status=404)

def run_server(port=8000):
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, RouterHandler)

    logger.info(f"Server starting on port {port}")
    print(f"Server running on port {port}...")
    print("Available routes:")
    for route in RouterHandler.routes:
        print(f"- http://localhost:{port}{route}")
        logger.info(f"Route available: http://localhost:{port}{route}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        shutdown_msg = "Server shutting down..."
        logger.info(shutdown_msg)
        print("\nShutting down server...")
        httpd.server_close()

if __name__ == '__main__':
    run_server(8050)
