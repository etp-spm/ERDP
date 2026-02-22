DP79e79016 - telemetry data
SRC437772e6 - system prod
“spire.source.datapoint”
or
“spire.*.datapoint>” if no source exists

python subscribe_script.py --server nats://streaming.us-east-1.nextgen.nascarracedata.com:4222 --creds Spire_Creds_020426/spire_prod/spire.creds --tlsca Spire_Creds_020426/spire_prod/ca_file --subscription "spire.>"

python subscribe_script.py --server nats://streaming.us-east-1.nextgen.nascarracedata.com:4222 --creds Spire_Creds_020426/spire_prod/spire.creds --tlsca Spire_Creds_020426/spire_prod/ca_file --subscription "spire.SRCf003cbe5.DP79e79016"


I'll update subscribe_parquet.py to write files in the {name}_{vehicle_id}.parquet format that the plotting script expects. This eliminates the watch processor entirely - direct NATS to parquet to plot.

Done! Now here's how it works with just 2 terminals:

Terminal 1 - Subscribe & Write Parquet
python subscribe_parquet.py --server nats://streaming.us-east-1.nextgen.nascarracedata.com:4222 --creds Spire_Creds_020426/spire_prod/spire.creds --tlsca Spire_Creds_020426/spire_prod/ca_file --subscription "spire.>"

Terminal 2 - Real-Time Plot
# See what data is available
python realtime_plot_dash.py --list

# Plot specific data
python realtime_plot_dash.py --name aSteering --vehicles 71 77

Then open http://127.0.0.1:8050



python realtime_plot_dash.py --output "D:\26DAY1_Heat1" --name Speed --vehicles 6

python realtime_plot_dash.py -d D:\26DAY1_Heat1 -n result_6 -v 7 -f spd --lap