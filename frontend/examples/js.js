var top_k_ips = []
var top_k_protocols = []
var protocols_stddev = []
var ips_stddev = []
var protocols_H = []
var ips_H = []

// var BACKEND_IP = "10.104.185.95"
var BACKEND_IP = "127.0.0.1"
var BACKEND_PORT = "5000"

function predict(query_id, H, T, limit=200) {
	if (limit == 0)
		return
	axios({
      url: "http://" + BACKEND_IP + ":" + BACKEND_PORT + "/api/"+query_id+"/"+H+"/"+T,
      method: 'get',
  }).then(response => {
      console.log(response);
      $('.out').remove()
      if (jQuery.isEmptyObject(response['data'])) {
          predict(query_id, H, T, limit-1)
      }
      else {
        prepareData(query_id, response)
				renderGraph(query_id)	
			}
  } ).catch(error => {
      console.log(error)
  })
}

// update graph periodically
function updateGraph() {
    
}

// prepare data periodically
function updateDictData() {

}

function renderGraph(query_id) {
    switch(Number(query_id)) {
		case 1:
			title = "top_protocol_H_T"
			ylabel = "bandwidth"
			var ctx = $('#top_protocol_H_T').get(0).getContext('2d')
			renderPieGraph(ctx, protocols_H, ylabel)
			break
		case 2:
			title = "top_k_protocols_T"
			protocols_wc.update(top_k_protocols);
			break
		case 3:
			title = "protocols_x_more_than_stddev"
			ylabel = "bandwidth"
			var ctx = $('#protocols_x_more_than_stddev').get(0).getContext('2d')
			renderBarGraph(ctx, ips_stddev, ylabel)
			break
		case 4:
			title = "top_ip_addr_H_T"
			ylabel = "bandwidth"
			var ctx = $('#top_ip_addr_H_T').get(0).getContext('2d')
			// sort ips_H to filter out some legend with less percentage in graph
			ips_H.sort((a, b) => (a.size < b.size) ? 1 : -1)
			renderPieGraph(ctx, ips_H, ylabel)
			break
		case 5:
			title = "top_k_ip_T"
			ip_wc.update(top_k_ips)
			break
		case 6:
			title = "ip_x_more_than_stddev"
			ylabel = "bandwidth"
			var ctx = $('#ip_x_more_than_stddev').get(0).getContext('2d')
			renderBarGraph(ctx, protocols_stddev, ylabel)
			break
	}
}
	
function zip(x, y) {
    var result = {};
    for (var i = 0; i < x.length; i++)
         //result[x[i]] = y[i];
        result[text] = x[i];
	result[size] = y[i];
    return result;
}

// Refresh global graph data after querying backend
function prepareData(query_id, response) {
		switch(query_id) {
			case 1:
				protocols_H = createDictData(response)
				break
			case 2:
				top_k_protocols = createDictData(response)
				break
			case 3:
				ips_stddev = createDictData(response)
				break
			case 4:
				ips_H = createDictData(response)
				break
			case 5:
				top_k_ips = createDictData(response)
				break
			case 6:
				protocols_stddev = createDictData(response)
				break
		}
}

// data convert to this form: [{text: "aa", size: 1}, {}, {}]
function createDictData(response) {
	tmp_list = []
	for (var k in response["data"]){
		tmp_list.push({
			text: k,
			size: Number(response["data"][k])		
		})
	}
	return tmp_list
}

// Show bar chart
// Data in form [{text: "aa", size: 1}, {}, {}]
function renderBarGraph(ctx, data, ylabel) {
	x = []
	y = []
	// flatten
	for (var i in data) {
		d = data[i]
		x.push(Object.values(d)[0])
		y.push(Object.values(d)[1])
	}
	var myChart = new Chart(ctx, {
		type: 'bar',
		data: {
		labels: x,
		datasets: [{
			label: ylabel,
			data: y,
			backgroundColor: [
			'rgba(255, 99, 132, 0.2)',
			'rgba(54, 162, 235, 0.2)',
			'rgba(255, 206, 86, 0.2)',
			'rgba(75, 192, 192, 0.2)',
			'rgba(153, 102, 255, 0.2)',
			'rgba(255, 159, 64, 0.2)'
			],
			borderColor: [
			'rgba(255, 99, 132, 1)',
			'rgba(54, 162, 235, 1)',
			'rgba(255, 206, 86, 1)',
			'rgba(75, 192, 192, 1)',
			'rgba(153, 102, 255, 1)',
			'rgba(255, 159, 64, 1)'
			],
			borderWidth: 1
		}]
		},
		options: {
		scales: {
			yAxes: [{
			ticks: {
				beginAtZero: true
			}
			}]
		},
		title: {
            display: false,
            text: title
        }
		}
	});
}

// Show pie chart
// Data in form [{text: "aa", size: 1}, {}, {}]
function renderPieGraph(ctx, data, ylabel) {
	console.log(data)
	x = []
	y = []
	// flatten
	for (var i in data) {
		d = data[i]
		x.push(Object.values(d)[0])
		y.push(Object.values(d)[1])
	}

	var myPieChart = new Chart(ctx, {
    type: 'pie',
    data: {
	labels: x,
	datasets: [{
			    label: ylabel,
			    data: y,
			    backgroundColor: [
				'rgba(255, 99, 132, 0.2)',
				'rgba(54, 162, 235, 0.2)',
				'rgba(255, 206, 86, 0.2)',
				'rgba(75, 192, 192, 0.2)',
				'rgba(153, 102, 255, 0.2)',
				'rgba(255, 159, 64, 0.2)'
			    ],
			    borderColor: [
				'rgba(255, 99, 132, 1)',
				'rgba(54, 162, 235, 1)',
				'rgba(255, 206, 86, 1)',
				'rgba(75, 192, 192, 1)',
				'rgba(153, 102, 255, 1)',
				'rgba(255, 159, 64, 1)'
			    ],
			    borderWidth: 1
			}]	
	
	},
    options: {
			title: {
				display: false
			},
			legend: {
		    display: true,
		    labels: {
		         filter: function(legendItem, data) {
		              return legendItem.index < 5
		         }
		    }
			}
		}	
	});
}

function send() {
    var query_id = $(".query").val()
    console.log(query_id)
    var H = $(".H").val()
    var T = $(".T").val()
    console.log(H)
    console.log(T)
    predict(query_id, H, T);
}

// get first batch of data
function initGraph() {
	predict(2, 10, 15)
	// protocols_wc.update([{text:"aa", size:90},{text:"ab", size:80},{text: "cc", size:70}])
	//console.log([{text:"aa", size:90},{text:"ab", size:80},{text: "cc", size:70}])
	// protocols_wc.update(top_k_protocols)
	predict(5, 10, 15)
	predict(1, 0.1, 15)
	predict(4, 0.1, 15)
	predict(3, 1, 15)
	predict(6, 1, 15)
}


initGraph()

