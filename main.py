import web
import json
import sys
import codecs
import tools

urls = (
        '?id1=(\d+)&id2=(\d+)', 'Result'
)

class Result:
        def GET(self, id1, id2):
            return tools.dealIdToId(id1, id2)

app = web.application(urls, globals())
application = app.wsgifunc()

if __name__ == "__main__"
        app.run()


