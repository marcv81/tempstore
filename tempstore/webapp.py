import tempstore.engine as ts_e

import werkzeug.exceptions
import werkzeug.routing
import werkzeug.utils
import werkzeug.wrappers
import werkzeug.wsgi

import jinja2
import traceback

# Base class for WSGI apps.
class BaseApp:

    def __init__(self, base_url):
        # Initializes the base URL.
        self.base_url = base_url
        # Initializes the Jinja2 environment.
        self.jinja2_environment = jinja2.Environment(
            trim_blocks=True,
            loader=jinja2.FileSystemLoader('views'))
        # Initializes the URL map.
        self.url_map = werkzeug.routing.Map([])

    # WSGI entry point.
    def __call__(self, environ, start_response):
        request = werkzeug.wrappers.Request(environ)
        response = self.route(request)
        return response(environ, start_response)

    # Routes the requests according to the URL map.
    def route(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, endpoint)(request, **values)
        # Catches routing exceptions.
        except werkzeug.exceptions.NotFound:
            return werkzeug.wrappers.Response(status=404)
        # Catches other exceptions.
        except Exception as e:
            traceback.print_exc()
            return werkzeug.wrappers.Response(status=500)

    # Returns a response instantiated from a template and parameters.
    def response_template(self, template_file, **kwargs):
        template = self.jinja2_environment.get_template(template_file)
        kwargs['base_url'] = self.base_url
        return werkzeug.wrappers.Response(
            template.render(**kwargs), mimetype='text/html')

    # Returns a response that redirects to another URL.
    def response_redirect(self, url):
        return werkzeug.utils.redirect(self.base_url + url)

class App(BaseApp):

    def __init__(self, engine, base_url):
        # Calls the parent constructor.
        BaseApp.__init__(self, base_url)
        # Initializes the engine.
        self.engine = engine
        # Adds the common routes to the URL map.
        self.url_map.add(werkzeug.routing.Rule(
            '/',
            methods=['GET'],
            endpoint='index'))
        self.url_map.add(werkzeug.routing.Rule(
            '/project/<project_name>',
            methods=['GET'],
            endpoint='project'))
        self.url_map.add(werkzeug.routing.Rule(
            '/version/<project_name>/<version_name>',
            methods=['GET'],
            endpoint='version'))
        self.url_map.add(werkzeug.routing.Rule(
            '/download/<project_name>/<version_name>/<file_name>',
            methods=['GET'],
            endpoint='download'))
        self.url_map.add(werkzeug.routing.Rule(
            '/admin/star/<project_name>/<version_name>',
            methods=['GET'],
            endpoint='star'))
        self.url_map.add(werkzeug.routing.Rule(
            '/admin/unstar/<project_name>/<version_name>',
            methods=['GET'],
            endpoint='unstar'))
        self.url_map.add(werkzeug.routing.Rule(
            '/upload',
            methods=['POST'],
            endpoint='upload'))

    # Home page.
    # Shows the projects list.
    def index(self, request):
        projects = self.engine.list_projects()
        return self.response_template(
            template_file='index.html',
            projects=projects)

    # Project page.
    # Shows the project versions.
    def project(self, request, project_name):
        versions = self.engine.list_versions(project_name)
        return self.response_template(
            template_file='project.html',
            project=project_name,
            versions=versions)

    # Version page.
    # Shows the version files.
    def version(self, request, project_name, version_name):
        files = self.engine.list_files(project_name, version_name)
        return self.response_template(
            template_file='version.html',
            project=project_name,
            version=version_name,
            files=files)

    # Download URL.
    # Passes the requested file to the client.
    def download(
            self, request,
            project_name, version_name, file_name):
        stream = self.engine.download(
            project_name, version_name, file_name)
        return werkzeug.wrappers.Response(
            werkzeug.wsgi.wrap_file(request.environ, stream),
            direct_passthrough=True,
            mimetype='application/octet-stream')

    # Star URL.
    # Processes the star and redirects to the project page.
    def star(self, request, project_name, version_name):
        self.engine.star_version(project_name, version_name)
        return self.response_redirect('/project/' + project_name)

    # Unstar URL.
    # Processes the star and redirects to the project page.
    def unstar(self, request, project_name, version_name):
        self.engine.unstar_version(project_name, version_name)
        return self.response_redirect('/project/' + project_name)

    # Upload URL.
    # Processes the file upload and redirects to the home page.
    def upload(self, request):
        # Extracts the parameters from the POST request.
        project_name = request.form.get('project')
        version_name = request.form.get('version')
        upload = request.files.get('upload')
        file_name = upload.filename
        # Performs the upload.
        self.engine.upload(
            project_name, version_name, file_name, upload)
        return self.response_redirect('/')
