# save matching urls in urls.py

from mitmproxy import http
import json

class URLLogger:
    def __init__(self, logfile: str = "urls.log"):
        self.logfile = logfile

    def request(self, flow: http.HTTPFlow):
        # Get the full URL (including query string)
        url = flow.request.pretty_url
        if url.startswith("https://cdn-mobapi.bloomberg.com/wssmobile"): 
            # get the headers 
            headers = flow.request.headers
            
            # save to file 
            with open('bloomberg.cookies.json', 'w') as f: 
                f.write(json.dumps(dict(headers)))
            
        # Append to file
        with open(self.logfile, "a") as f:
            f.write(url + "\n")

# Register the addon
addons = [
    URLLogger()
]
