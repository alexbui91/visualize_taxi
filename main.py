import os, os.path
import string

import cherrypy
import get_data as g
import json


class WebApp(object):
    
    @cherrypy.expose
    def index(self):
        return open('index.html')
    index.exposed = True


@cherrypy.expose
class Router(object):

    @cherrypy.tools.accept(media='text/plain')
    @cherrypy.expose
    def POST(self, taxi=0):
        print(int(taxi))
        data = g.get_all_latlng(int(taxi))
        return json.dumps({'data': data})


if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/get_path': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        },
        '/fonts': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public/fonts'
        }
    }
    g.init_spark()
    webapp = WebApp()
    webapp.get_path = Router()
    cherrypy.quickstart(webapp, '/', conf)
