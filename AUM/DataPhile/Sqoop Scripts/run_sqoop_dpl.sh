#!/bin/bash
#export JAVA_HOME=/usr/jdk64/current
root_path="/data-01/MFCGD.COM/invaumsvcqa/DataPhile"
log_path="${root_path}/logs"

msi_conn="jdbc:datadirect:openedge://10.201.170.28:1692;databaseName=exc2"
msisi_conn="jdbc:datadirect:openedge://10.201.170.28:3092;databaseName=exc2"
msii_conn="jdbc:datadirect:openedge://10.201.170.28:1792;databaseName=exc2"
vm_conn="jdbc:datadirect:openedge://10.201.170.28:1693;databaseName=exc2sm"

actran='2020-05-17'
gnexch='2020-05-17'
tpkptl='2020-05-17'
tpcont='2020-05-17'
mfrrus='2020-05-17'
process_date='2020-05-02'


export actran
export gnexch
export tpkptl
export tpcont
export mfrrus
export process_date

export root_path
export log_path

export msi_conn
export msisi_conn
export msii_conn
export vm_conn


./dataPhile_sqoop_import_msi.sh
