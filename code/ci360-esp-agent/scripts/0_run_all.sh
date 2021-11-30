
echo 
echo Start ESP
./1_start_esp_server.sh
sleep 3
echo 
echo Load ESP project
./2_load_esp_project.sh
sleep 2
echo 
echo Run CAS Adapter
./3_run_cas_adapter.sh
sleep 1
echo
echo Start CI360 Agent
./4_start_ci360_esp_agent.sh
echo
echo Show Status
./status.sh
