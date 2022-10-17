# Belt-and-Light-Sensor (BLS) System Mock-Up

The server is a mock-up of a part of the liquid-liquid extraction (LLE) system. Endpoints are documented in the table below:

| Verb | URL | Parameters | Request Body | Response | Description |
| --- | --- | --- | --- | --- | --- |
| GET | /start |  |  | {"status": string, "settings":{"sleep_delay": integer},"results": [][]integer} | Starts the experiment |
| POST | /start |  | {"sleep_delay": integer} | {"status": string, "settings":{"sleep_delay": integer},"results": [][]integer} | Starts the experiment. “sleep_delay” imitates some work being done on the server for this amount of seconds |
| GET | /status |  |  | {"status": string, "settings":{"sleep_delay": integer},"results": [][]integer} | Returns the current status of the server altogether with settings and results if available |
| GET | /stop |  |  | {"status": string, "settings":{"sleep_delay": integer},"results": [][]integer} | Stops the experiment and removes all the temporary data |
| GET | /results |  |  | {"status": string, "settings":{"sleep_delay": integer},"results": [][]integer} | Returns the results of the experiment |

## Getting Started

Run the following command in the root directory of the repository:

```shell
$ uvicorn main:app --reload
```
