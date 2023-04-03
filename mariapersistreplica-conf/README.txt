To set up the replicae, run something like this on the server:

CHANGE MASTER TO
  MASTER_HOST='mariapersist',
  MASTER_USER='mariapersist',
  MASTER_PASSWORD='password',
  MASTER_PORT=3333,
  MASTER_CONNECT_RETRY=10;

START SLAVE;

SHOW SLAVE STATUS;
