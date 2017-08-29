import os
import sys

import string

import cherrypy

import json

import host as h



class WebApp(object):
    
    @cherrypy.expose
    def index(self):
        return open('index.html')

    @cherrypy.expose
    def heatmap(self):
        return open('heatmap.html')
    # index.exposed = True


@cherrypy.expose
class Router(object):

    @cherrypy.tools.accept(media='text/plain')
    @cherrypy.expose
    def POST(self, taxi=0):
        idx = int(taxi)
        data = g.get_all_latlng(idx)
        speed = g.get_average_speed(idx)
        distance = g.get_total_running(idx)
        return json.dumps({'data': data, 'avg_speed': speed.speed, 'total_distance' : distance.distance})


@cherrypy.expose
class HeatMap(object):

    @cherrypy.tools.accept(media='text/plain')
    @cherrypy.expose
    def POST(self, tfrom=0, tto=0, date=0, station_type="[0,1,2,3]", usage=2, boundary="[]", threshold=0):
        st = json.loads(station_type)
        bd = json.loads(boundary)
        data = s.get_points(tfrom, tto, int(date), st, usage, bd, int(threshold))
        return json.dumps({'data': data})

@cherrypy.expose
class Station(object):

    @cherrypy.tools.accept(media='text/plain')
    @cherrypy.expose
    def POST(self):
        data = s.get_subwaypoints()
        if data:
            curr_line = -1
            curr_line_data = None
            tmp = dict()
            for x in data:
                if not curr_line is x.line:
                    if curr_line_data:
                        tmp[curr_line] = curr_line_data
                    curr_line = x.line
                    curr_line_data = list()
                curr_line_data.append(x)
            tmp[curr_line] = curr_line_data
            data = tmp
        else:
            data = []
        return json.dumps({'data': data})

if __name__ == '__main__':

    dir_path = os.path.dirname(os.path.realpath(__file__))
    activate_this = "%s/.env/bin/activate_this.py" % dir_path

    if os.path.exists(activate_this):
        execfile(activate_this, dict(__file__=activate_this))

    import get_data as g
    #import get_smartcard as s
    import get_smartcard_0823 as s
    cherrypy.config.update({
        'server.socket_host': h.host,
        'server.socket_port': h.port,
    })

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
        '/get_heat_data': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json'), ("Access-Control-Allow-Origin", '*')],
        },
        '/get_subwaypoints': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json'), ("Access-Control-Allow-Origin", '*')],
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
    # g.init_spark()
    # s.init_spark()
    # s.load_data()
    webapp = WebApp()
    webapp.get_path = Router()
    webapp.get_heat_data = HeatMap()
    webapp.get_subwaypoints = Station()
    cherrypy.quickstart(webapp, '/', conf)

