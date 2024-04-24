# LinkedIn Jobs Data Scrapper
This is a stub

## DevOPS
Use this command to inhibit system from going to sleep while running the process (Bash and need the venv):
```bash
systemd-inhibit --what=sleep:handle-lid-switch python -m run
```
### AWS
#### Logging and Metrics
You must use Amazon CloudWatch Agent to monitor system metrics and logs:
1. First, download and install Amazon CloudWatch Package (You should use `wget` to
   download the package on Ubuntu).
2. You need to attach `CloudWatchAgentServerPolicy` IAM role to you target ec2 machine
3. cd `opt/aws/amazon-cloudwatch-agent/bin` then Run
`amazon-cloudwatch-agent-config-wizard` To auto-configure your config.json
_Important Note_: Make sure to set the user to `root`. Otherwise the agent won't be able to read the log files.
4. run:
```bash
sudo ./amazon-cloudwatch-agent-ctl -a start -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json
```
5. When you edit or change the `config.json` file, make sure to run:
```bash
sudo ./amazon-cloudwatch-agent-ctl -a fetch-config -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json -s
```
6. To monitor specific processes, you can use `procstat` extension to CloudWatch.

The sample config file:
```json
{
	"agent": {
		"metrics_collection_interval": 60,
		"run_as_user": "root"
	},
	"logs": {
		"logs_collected": {
			"files": {
				"collect_list": [
					{
						"file_path": "/home/ubuntu/scrapper/log/run.log",
						"log_group_class": "STANDARD",
						"log_group_name": "my_log",
						"log_stream_name": "scrapper-{instance_id}",
						"retention_in_days": 5
					}
				]
			}
		}
	},
	"metrics": {
		"aggregation_dimensions": [
			[
				"InstanceId"
			]
		],
		"append_dimensions": {
			"AutoScalingGroupName": "${aws:AutoScalingGroupName}",
			"ImageId": "${aws:ImageId}",
			"InstanceId": "${aws:InstanceId}",
			"InstanceType": "${aws:InstanceType}"
		},
		"metrics_collected": {
			"mem": {
				"measurement": [
					"mem_used_percent"
				],
				"metrics_collection_interval": 60
			},
			"statsd": {
				"metrics_aggregation_interval": 60,
				"metrics_collection_interval": 10,
				"service_address": ":8125"
			},
			"procstat" : [
				{
					"exe": "chrome",
					"measurement" : [
						"cpu_usage",
						"cpu_time"
					]
				}

			]
		}
	}
}
```

### CPU utilization of Chrome

The Google Chrome specially in Headless mode is notorious for getting out-of-control in
cpu and mem usage. Since I'm using ec2-micro, this can become critical easily. The hard
approach is the cap the cpu and mem is to use `cgroup`. [(e.g. see this)](https://askubuntu.com/questions/1377502/limit-cpu-and-memory-using-cgroup-in-ubuntu-20-04-lts-server-edition).

The easy way is to use the package `cpulimit`. However cpulimit only limit one process at a time (since chrome spans multiple processes, this isn't too effective). But there's an excellent [bash file](https://aweirdimagination.net/2020/08/09/limit-processor-usage-of-multiple-processes/) that wraps this package and makes it possible to use cpulimit to limit multiple processes. 
Example:
```bash
./cpulimit-all.sh -l 20 -e chrome --max-depth=3 --watch-interval=1
```
Note that chrome spawns many processes, the 30% cap, limits each individual process. You can't dial it much higher because many individual processes sum up and take all the cpu capacity.