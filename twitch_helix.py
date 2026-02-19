import asyncio
import datetime
import logging

import aiohttp


logger = logging.getLogger("Helix")


class HelixClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._token_expires_at: datetime.datetime | None = None

    async def _ensure_token(self):
        if self._token and self._token_expires_at:
            if datetime.datetime.now() < (self._token_expires_at - datetime.timedelta(seconds=30)):
                return
        await self._fetch_app_token()

    async def _fetch_app_token(self):
        url = "https://id.twitch.tv/oauth2/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        delay = 1
        for attempt in range(6):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        payload = await resp.json()
                        if resp.status != 200:
                            raise RuntimeError(f"token_http_{resp.status}: {payload}")

                token = payload.get("access_token")
                expires_in = int(payload.get("expires_in", 0))
                if not token or expires_in <= 0:
                    raise RuntimeError(f"bad_token_payload: {payload}")

                self._token = token
                self._token_expires_at = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                logger.info("Helix: app access token получен")
                return
            except Exception as e:
                logger.error(f"Helix: не удалось получить app token (attempt={attempt + 1}): {e}")
                await asyncio.sleep(min(30, delay))
                delay = min(30, delay * 2)

        raise RuntimeError("Helix: app token не получен после ретраев")

    async def is_stream_online(self, user_login: str) -> bool:
        await self._ensure_token()
        url = "https://api.twitch.tv/helix/streams"
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._token}",
        }
        params = {"user_login": user_login}

        delay = 1
        for attempt in range(6):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        payload = await resp.json()

                        if resp.status == 401:
                            self._token = None
                            self._token_expires_at = None
                            await self._ensure_token()
                            headers["Authorization"] = f"Bearer {self._token}"
                            continue

                        if resp.status != 200:
                            raise RuntimeError(f"streams_http_{resp.status}: {payload}")

                data = payload.get("data", [])
                return bool(data)
            except Exception as e:
                logger.error(f"Helix: ошибка GET /streams (attempt={attempt + 1}): {e}")
                await asyncio.sleep(min(30, delay))
                delay = min(30, delay * 2)

        return False

    async def get_user_id(self, user_login: str) -> str | None:
        await self._ensure_token()
        url = "https://api.twitch.tv/helix/users"
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._token}",
        }
        params = {"login": user_login}

        delay = 1
        for attempt in range(6):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        payload = await resp.json()

                        if resp.status == 401:
                            self._token = None
                            self._token_expires_at = None
                            await self._ensure_token()
                            headers["Authorization"] = f"Bearer {self._token}"
                            continue

                        if resp.status != 200:
                            raise RuntimeError(f"users_http_{resp.status}: {payload}")

                data = payload.get("data", [])
                if not data:
                    return None
                return data[0].get("id")
            except Exception as e:
                logger.error(f"Helix: ошибка GET /users (attempt={attempt + 1}): {e}")
                await asyncio.sleep(min(30, delay))
                delay = min(30, delay * 2)

        return None
