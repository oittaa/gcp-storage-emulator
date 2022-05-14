### Error Handling for each route:

- ls:
    - bucket not found                                  404
    - success                                           200

- getbyid:
    - bucket not found                                  404
    - notification not found                            404
    - success                                           200

- insert:
    - bucket not found                                  404
    - mising topic                                      400
    - missing or invalid value for payload format       400
    - invalid topic                                     400
    - topic not found                                   404
    - success                                           201

- delete:
    - bucket not found                                  404
    - notification not found                            404
    - success                                           204
