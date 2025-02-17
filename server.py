
import cgi
import io
import json
import pandas as pd
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from app.az import *
from app.utils import get_last_value, get_date_file_with_type


class RouterHandler(BaseHTTPRequestHandler):
    # Dictionary to store route handlers

    get_routes = {
        '/experiments': 'handle_home',
        '/experiments/api/users': 'handle_users',
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

    def do_GET(self):
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

    # Route handlers
    def handle_home(self, params):
        content = """
        <h1>Welcome to the Home Page</h1>
        <p>Try visiting /about or /api/users</p>
        """
        self.send_response_content(content)

    def handle_users(self, params):
        users = ["Alice", "Bob", "Charlie"]
        content = {
            "users": users,
            "count": len(users)
        }
        self.send_response_content(str(content), content_type='application/json')

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

            # Process the file based on its type
            if file_extension == '.csv':
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                raise ValueError(f"Unknown action: {action}")

            return df
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sb_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = upload_sb_data(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_map_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = upload_asin_cat_map(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_campaign_map_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = upload_campaign_cat_map(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sp_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = upload_sp_data(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_sd_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = upload_sd_data(calculated_df, "Himanshu")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_az_br_upload(self, params):
        try:
            calculated_df = self._get_df_from_upload(params)

            response_data = []

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_dashboard_metrics(self, params):
        try:
            print(1.1)
            calculated_df = self._get_df_from_upload(params)
            v = get_date_file_with_type(
                "Himanshu", "sponsored_products", "2025-01-04", "2025-01-28")
            print(1.2)

            response_data = calculate_complete_category_metrics(
                calculated_df, "Himanshu", "2025-01-01", "2025-01-31")

            self.send_response_content(
                json.dumps(response_data, indent=2),
                content_type='application/json'
            )
        except Exception as e:
            error_response = {"error": str(e)}
            self.send_response_content(
                json.dumps(error_response),
                status=400,
                content_type='application/json'
            )

    def handle_404(self, params=None):
        content = "<h1>404 Not Found</h1><p>The requested route does not exist.</p>"
        self.send_response_content(content, status=404)

def run_server(port=8000):
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, RouterHandler)

    print(f"Server running on port {port}...")
    print("Available routes:")
    for route in RouterHandler.routes:
        print(f"- http://localhost:{port}{route}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.server_close()

if __name__ == '__main__':
    run_server(8050)
