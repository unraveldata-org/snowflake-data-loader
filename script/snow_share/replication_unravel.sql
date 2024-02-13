
--Step-1 (this is not for customer) Receive the data in recipient account (Sql mode).
--Validate that the inbound share is available to the consumer account.
Show shares like '%UNRAVEL%';
Use role AccountAdmin;
create database ${CUSTOMER_NAME}_SHARE from share
FWTTICE.PRIMARY_PG.${CUSTOMER_NAME}_UNRAVEL_SHARE;