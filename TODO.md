get:
    -

ls:
    -


insert:
    - check if bucket exists `CONFLICT`
    - check topic inside request.data `BAD_REQUEST`
    - check payload_format inside request.data `BAD_REQUEST`
    - check topic string format `BAD_REQUEST`
    - check payload_format possible values `BAD_REQUEST`
    - check if topic exists `CONFLICT`

delete:
    -