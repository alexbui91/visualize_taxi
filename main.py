import os
import sys
import time
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
    def POST(self, time="[]", date=0, station_type="[0,1,2,3]", usage=2, boundary="[]", threshold=0, pair_threshold=0):
        tm = json.loads(time)
        st = json.loads(station_type)
        bd = json.loads(boundary)
	    #start = time.time()    
        data1 = s1.get_points(tm[0], tm[1], int(date), st, usage, bd, int(threshold))
        data2 = s2.get_points(tm[0], tm[1], int(date), st, usage, bd, int(pair_threshold))
        #end = time.time()
	#print("query time: %.2f\n" % (end-start))
	return json.dumps({'data1': data1, 'data2': data2})


if __name__ == '__main__':

    dir_path = os.path.dirname(os.path.realpath(__file__))
    activate_this = "%s/.env/bin/activate_this.py" % dir_path

    if os.path.exists(activate_this):
        execfile(activate_this, dict(__file__=activate_this))

    import get_data as g
    import get_smartcard as s1
    import get_smartcard_v2 as s2
    #import get_smartcard_0823 as s
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
    spark = s1.init_spark()
    s1.load_data(spark)
    s2.load_data(spark)
    webapp = WebApp()
    webapp.get_path = Router()
    webapp.get_heat_data = HeatMap()
    webapp.get_subwaypoints = Station()
    cherrypy.quickstart(webapp, '/', conf)

