* proxy
    1. wireguard `https://www.digitalocean.com/community/tutorials/how-to-set-up-wireguard-on-ubuntu-20-04`
    2. start `systemctl enable wg-quick@wg0.service`
        `systemctl status wg-quick@wg0.service`
    3. `systemctl enable mitmproxy.service` 
        `systemctl status mitmproxy`
    4. `start.sh` lets requests outside bloomberg.com automatically go through 
    5. Open UDP port 80

* Folder structure: /backend /venv 
    1. setup venv: `python3 -m venv venv`
    2. load python: `source ./venv/bin/activate`
    3. `python -m pip install --upgrade pip`
    4. `python -m pip install -r requirements.txt`

* service files 
    1. located in `/etc/systemd/system`
    2. `rflx.service` fetches news 
    3. `fastapi.service` starts the server
    4. `mitmproxy.service` news sites cookies

* restart the services: 
    1. `sudo systemctl daemon-reload` (loads new changes)

    2. `systemctl restart rflx` (restarts the actual service)
    3. `systemctl restart fastapi`

    4. `systemctl status rflx` (check if restarted successfully)

* connect to remote
    1. `ssh root@91.99.222.135`
    2. Forward port via vscode

* pre-production changes
    1. Set base url in `frontend/api.ts` to `https://api.reflexia.markets`