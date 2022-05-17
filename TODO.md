### Response Handling for each route:

- ls:
    - bucket not found                                  404
    - success                                           200

- get:
    - bucket not found                                  404
    - notification not found                            404
    - success                                           200

- delete:
    - bucket not found                                  404
    - notification not found                            404
    - success                                           204


### Mapping api structure

Server -> APIThread -> RequestHandler -> Router -> Handlers, Request, Response
													|
													V
											       Controllers -> Services -> Storage