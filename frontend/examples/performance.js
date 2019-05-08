function renderLineChart(ctx, datasets, xlabel, ylabel, datalabel) {
	
	var myLineChart = new Chart(ctx, {
    type: 'line',
    data: {
		// labels: xlabels,
		datasets: [{	
					label: ylabel,
					data: points,
					borderColor: "#f4d942",
					fill: false,      // disable fill under line
					borderWidth: 5
				}]
		},
    options: {
			scales: {
        xAxes: [{
 					scaleLabel: {
				    display: true,
				    labelString: xlabel,
						fontSize: 15
				  },
					type: 'linear',
					position: 'bottom'     
        }],
				yAxes: [{
				  scaleLabel: {
				    display: true,
				    labelString: ylabel,
						fontSize: 15
				  }
    		}]
      },
			legend: {
				display: false
			}
		}
	});

}

function initGraphs() {
	c = [5.375, 12.000, 23.312, 29.800, 49.545, 94.500, 189.000]
	l = [1.833, 2.000, 2.5000, 3.900, 4.000, 4.1000, 4.333]
	t = [0.546, 0.500, 0.400, 0.31, 0.25, 0.244, 0.231]
	tmp = []
	tmp2 = []
	for (var i in l) {
		tmp.push({
			x: c[i],
			y: l[i]
		})
		tmp2.push({
			x: c[i],
			y: t[i]
		})
	}
	console.log(tmp)
	console.log(tmp2)

	var ctx = $('#latency').get(0).getContext('2d')

	var myLineChart = new Chart(ctx, {
		type: 'line',
		data: {
		// labels: xlabels,
		datasets: [
				{
					label: "Throughput",
					data: tmp2,
					borderColor: "	#98f442",
					fill: false,      // disable fill under line
					borderWidth: 5				
				}]
		},
		options: {
			scales: {
		    xAxes: [{
					scaleLabel: {
						display: true,
						labelString: "Complexity",
						fontSize: 15
					},
					type: 'linear',
					position: 'bottom'     
		    }],
				yAxes: [{
					scaleLabel: {
						display: true,
						labelString: "Latency",
						fontSize: 15
					}
				}
				]
		  },
			legend: {
				display: true
			}
		}
	});
	
	// renderLineChart(ctx, tmp, "Complexity", "Latency")
}

initGraphs()
