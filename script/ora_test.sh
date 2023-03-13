#!/bin/bash

USERID=etladm
PASSWORD=etladm
DW_HOST=xe
echo sqlplus -s ${USERID}/${PASSWORD}@${DW_HOST}
sqlplus -s ${USERID}/${PASSWORD}@xe<<END
execute ${STG_SP_NAME};
commit;
END

