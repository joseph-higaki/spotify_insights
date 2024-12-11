# Manual start tests

Assume `/workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/` is empty

`cp --preserve=timestamps -r /workspaces/spotify_insights/0_tmp_data/const_tests/joseph-higaki/. /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/`
To preserve last acccessed timestamps



## Testing timestamp metadata of file

stat /workspaces/spotify_insights/0_tmp_data/const_tests/joseph-higaki/Streaming_History_Audio_2024_5.json

stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/Streaming_History_Audio_2024_5.json

cp --preserve=timestamps /workspaces/spotify_insights/0_tmp_data/const_tests/joseph-higaki/Streaming_History_Audio_2024_5.json /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json


stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json
rm /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json

### Tried to change system time

It didn't work, looks like before touch command is applied to the file, the system time had been automatically corrected already

```bash
NOW=$(date) 
sudo date -s "2024-12-09 21:30:11" 
touch /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json
stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json
sudo date -s "$NOW"
unset NOW
stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json

sudo date -s "2024-12-09 21:30:11" && touch /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json && sudo date -s "$NOW"
sudo date -s "2024-12-09 21:30:11" && touch /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json && stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json

sudo date -s "2024-12-09 21:30:11" && date && touch /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json && stat /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json
```
touch -d "2024-12-09 21:30:11"  /workspaces/spotify_insights/0_tmp_data/new/joseph-higaki/some_new_json

# Testing with docker
docker compose -f /workspaces/spotify_insights/airflow/docker-compose.yaml up
docker cp  /workspaces/spotify_insights/0_tmp_data/. airflow-airflow-scheduler-1:/opt/0_tmp_data/
docker compose -f /workspaces/spotify_insights/airflow/docker-compose.yaml down 

docker exec -it airflow-airflow-scheduler-1 sh
docker exec -u root -it airflow-airflow-scheduler-1 sh
rm -r /0_tmp_data



