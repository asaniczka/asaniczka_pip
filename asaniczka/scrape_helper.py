"""
This module provides some useful functions to test and build a scraper.

Get started with `from asaniczka import scraper_helper as helper`

## Available Functions:

1. help_forge_cookies()
2. check_ratelimit()
3. steal_cookies()

"""

from threading import Lock
import concurrent.futures
from typing import Optional, Union
import time
from enum import Enum

import requests
from tqdm.auto import tqdm
from playwright.sync_api import sync_playwright
import pydantic
import asaniczka.main as asaniczka

# ------------------------------------------------------------
#                           CLASSES
# ------------------------------------------------------------


class ProxyProvider(Enum):
    """Class for different proxy providers"""

    WEBSHARE = "webshare"


class Proxy:
    """
    General class to store and format proxies

    ### Responsibility:
    - Initialize a Proxy object with proxy data and provider
    - Parse a webshare proxy format
    - Convert the proxy to playwright proxy format
    - Convert the proxy to HTTP Basic Auth format

    ### Args:
    - `str_proxy`: proxy data as a string
    - `provider`: Provider of the proxy. Use ProxyProvider Enum

    ### Returns:
    - `to_playwright`: Return a dictionary containing the proxy data in playwright format
    - `to_basic_auth`: Return a dictionary containing the proxy data in HTTP Basic Auth format

    ### Raises:
    - `Error`: May raise errors if the proxy data is not in expected format

    """

    def __init__(self, str_proxy: str, provider: ProxyProvider) -> None:

        self.raw_string = str_proxy
        self.ip_address: str = None
        self.port: int = None
        self.username: str = None
        self.password: str = None

        if provider.value == ProxyProvider.WEBSHARE.value:
            self.parse_webshare()

    def parse_webshare(self) -> None:
        """
        Parse the webshare proxy format.

        ### Args:
        - No explicit arguments taken as input directly. It accesses class attributes.

        ### Returns:
        - Does not explicitly return anything but updates the class attributes with parsed proxy data.

        ### Raises:
        - `IndexError`: If the split_proxy list does not have enough elements to assign to ip_address, port, username, and password.
        """
        try:
            split_proxy = self.raw_string.split(":")
            self.ip_address = split_proxy[0]
            self.port = split_proxy[1]
            self.username = split_proxy[2]
            self.password = split_proxy[3]
        except Exception as e:
            print(e)
            self.ip_address = ""
            self.port = ""
            self.username = ""
            self.password = ""

    def to_playwright(self) -> dict:
        """
        Outputs the proxy to playwright proxy format.

        ### Args:
        - No explicit arguments taken as input directly. It accesses class attributes.

        ### Returns:
        - `to_playwright`: Returns a dictionary containing the proxy data in playwright format with keys server, username, and password.

        ### Raises:
        - No explicit raises.
        """

        return {
            "server": f"{self.ip_address}:{self.port}",
            "username": self.username,
            "password": self.password,
        }

    def to_basic_auth(self) -> dict:
        """
        Outputs the proxy to HTTP Basic Auth format.

        ### Args:
        - No explicit arguments taken as input directly. It accesses class attributes.

        ### Returns:
        - `to_basic_auth`: Returns the proxy data in HTTP Basic Auth format as a formatted string.

        ### Raises:
        - No explicit raises.
        """

        return f"http://{self.username}:{self.password}@{self.ip_address}:{self.port}"


# ------------------------------------------------------------
#                         FUNCTIONS
# ------------------------------------------------------------
def send_request(
    url: str,
    timer,
    count_lock: Lock,
    burst_data: dict,
    pbar: tqdm = None,
    check=True,
    headers=None,
    data=None,
    is_get=True,
) -> None | int:
    """
    Sends a request to the given URL and returns the response status code if required.

    ### Args:
    - `url`: The URL to send the request to.
    - `timer`: The timer object used for tracking time.
    - `count_lock`: The lock object for thread synchronization.
    - `burst_data`: The burst data dictionary containing information about the number of requests made and rate limit.
    - `pbar`: The progress bar instance to track the requests. (default: None)
    - `check`: Whether to check the response status code. If True, the function will return the status code. If False, the function will update the burst data and return None. (default: True)
    - `headers`: The headers to include in the request. If None, default headers will be used. (default: None)
    - `data`: The data to include in the request body. (default: None)
    - `is_get`: True if it is a GET request, False if it is a POST request. (default: True)

    ### Returns:
    - `None` or `int`: If check is True, returns the response status code. If check is False, returns None.

    ### Raises:
    - No explicit raises.
    """

    with count_lock:
        if pbar:
            pbar.update(1)
        if burst_data["requests_till_429"] != 0:
            return None

    if not headers:
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3",
            "accept": "application/json",
        }

    if is_get:
        response = requests.get(
            url,
            headers=headers,
            data=data,
            timeout=10,
        )
    else:
        response = requests.post(
            url,
            headers=headers,
            data=data,
            timeout=10,
        )

    if check:
        return response.status_code

    with count_lock:
        if response.status_code != 200:
            if burst_data["requests_till_429"] == 0:

                burst_data["requests_till_429"] = burst_data["total_requests"]
                burst_data["time_till_429"] = timer.lap(full_decimals=True)

        if burst_data["requests_till_429"] != 0:
            return None

        burst_data["total_requests"] += 1

    return None


def check_ratelimit(
    url: str,
    check=True,
    headers: Optional[Union[dict, None]] = None,
    data: Optional[Union[dict, str, None]] = None,
    is_get=True,
) -> str:
    """
    Use this function to check the ratelimit of a domain.

    ### Args:
    - `url`: The URL of the domain to check the ratelimit.
    - `check`: Whether to perform a check request. If True, the function will send a single request to check the status code of the URL. (default: True)
    - `headers`: The headers to include in the request. If None, default headers will be used. (default: None)
    - `data`: The data to include in the request body. (default: None)
    - `is_get`: True if it is a GET request, False if it is a POST request. (default: True)

    ### Returns:
    - `str`: A message containing information about the ratelimit status.

    ### Raises:
    - No explicit raises.
    """
    count_lock = Lock()
    burst_data = {"total_requests": 0, "requests_till_429": 0, "time_till_429": 0}
    timer = asaniczka.Stopwatch()

    if check:
        status_code = send_request(
            url,
            timer,
            count_lock,
            burst_data,
            headers=headers,
            data=data,
            is_get=is_get,
        )
        return f"We got {status_code} status for {url}. Now run with check=False"

    print("Bursting endpoint. Might take a minute or so")

    with tqdm(total=1000, unit=" requests") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as thread_executor:
            futures = []
            for _ in range(1000):
                future = thread_executor.submit(
                    send_request,
                    url,
                    timer,
                    count_lock,
                    burst_data,
                    pbar=pbar,
                    check=False,
                    headers=headers,
                    data=data,
                    is_get=is_get,
                )

                time.sleep(0.05)
                futures.append(future)

            if (
                burst_data["requests_till_429"] != 0
                and burst_data["time_till_429"] != 0
            ):
                thread_executor.shutdown(wait=False, cancel_futures=True)

            for future in concurrent.futures.as_completed(futures):
                try:
                    _ = future.result()
                # pylint:disable=bare-except
                except:
                    pass

    if burst_data["requests_till_429"] == 0:
        return_message = "Wow, we never hit the ratelimit after 1000 burst requests"

    elif burst_data["requests_till_429"] != 0 and burst_data["time_till_429"] != 0:

        return_message = f"We did {burst_data['requests_till_429']} in {burst_data['time_till_429']} sec before hitting the rate limit.\n\n"

        return_message += f"That's {burst_data['requests_till_429']/burst_data['time_till_429']:.0f} per second or {burst_data['requests_till_429']/burst_data['time_till_429']*60:.0f} per minute.\n\n"

        if burst_data["time_till_429"] <= 60 and burst_data["requests_till_429"] >= 30:
            return_message += (
                "Or maybe " + str(burst_data["requests_till_429"]) + " per minute"
            )

    else:
        return_message = f"Requests: {burst_data['requests_till_429']}, time: {burst_data['time_till_429']}"

    return return_message


def help_forge_cookies(url: str, project) -> None:
    """
    Use this function to help identify cookie variables for forging cookies.

    ### Args:
    - `url`: The URL to load in the browser and retrieve cookies from.
    - `project`: The project setup object for saving temporary files.

    ### Returns:
    - `None`

    ### Raises:
    - No explicit raises.
    """

    print("Loading browser and getting initial cookies")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url)
        cookies = page.context.cookies()

        print(f"Found {len(cookies)} cookies")

    stolen_cookie_dict = {}
    if cookies:
        for cookie in cookies:
            stolen_cookie_dict[cookie["name"]] = cookie["value"]

    api_cookie = str(input("Please enter api cookies:\n"))

    # replace cookie values with dict key names
    for key, value in stolen_cookie_dict.items():
        api_cookie = api_cookie.replace(str(value), f"<<stolen_cookie['{key}']>>")
        api_cookie = api_cookie.replace("<<", "{").replace(">>", "}")

    project.save_temp_file(stolen_cookie_dict, file_name="stolen_cookie_dict")
    project.save_temp_file(api_cookie, file_name="raw_api_cookie")

    print()
    print("COOKIES SAVED. Please check temp folder :)")


def steal_cookies(url: str, proxy: Proxy = None) -> dict:
    """
    Gets cookies from a given domain.

    ### Args:
    - `url`: The URL from which to steal cookies.
    - `proxy`: Proxy Class. Optional (default: None)

    ### Returns:
    - `dict`: A dictionary containing the stolen cookies, where the keys are the cookie names and the values are the cookie values.

    ### Raises:
    - `RuntimeError`: If an error occurs when stealing the cookies.

    """

    try:
        with sync_playwright() as pw:
            if proxy:
                browser = pw.chromium.launch(proxy=proxy.to_playwright())
                page = browser.new_page()

            else:
                browser = pw.chromium.launch()
                page = browser.new_page()

            page.goto(url)
            cookies = page.context.cookies()

        stolen_cookie_dict = {}
        if cookies:
            for cookie in cookies:
                stolen_cookie_dict[cookie["name"]] = cookie["value"]

        return stolen_cookie_dict

    except Exception as error:
        raise RuntimeError(
            f"Error when stealing cookies. Please inform developer (asaniczka@gmail.com) of this error as this error is not handled. \n{asaniczka.format_error(error)}"
        ) from error


def validate_proxies(proxies: list[Proxy]) -> list[Proxy]:
    """
    Checks if the provided proxies are live and returns only the live ones
    """

    def send_dummy_request(proxy: Proxy) -> Proxy | None:
        response = asaniczka.get_request(
            "https://api.ipify.org",
            silence_exceptions=True,
            timeout=5,
            proxy=proxy.to_basic_auth(),
        )

        if response:
            return proxy
        return None

    working_proxies = []
    with concurrent.futures.ThreadPoolExecutor() as thread_executor:
        futures = []

        for proxy in proxies:
            future = thread_executor.submit(send_dummy_request, proxy)

            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                working_proxies.append(result)

    return working_proxies


def download_proxies(url: str, validate=True) -> list[Proxy]:
    """
    Downloads the proxies from your webshare live url
    """

    if not url:
        raise ValueError("No URL provided")

    if not url.startswith("https://proxy.webshare.io"):
        raise ValueError("We only accept webshare URLS")

    response = asaniczka.get_request(url, timeout=5)

    if not response:
        raise ValueError("URL didn't recieve any data")

    lines = response.split("\n")
    lines = [line.strip() for line in lines if ":" in line]
    proxies = [Proxy(line, ProxyProvider.WEBSHARE) for line in lines]

    if validate:
        working_proxies = validate_proxies(proxies)
        print("You have ", len(working_proxies), " working proxies!")
    else:
        working_proxies = proxies

    return working_proxies
