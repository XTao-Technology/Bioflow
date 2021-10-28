FROM postgresql:latest
MAINTAINER Javen Wu <javen.wu@xtaotech.com>


ADD ./bioflow.sql /bioflow.sql
ADD ./start_bioflow_db.sh /start_bioflow_db.sh

RUN chmod +x /start_bioflow_db.sh

#Run pgEngine
CMD ["/start_bioflow_db.sh"]
