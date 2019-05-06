function predict(query_id, H, T, limit=200) {
	if (limit == 0)
		return
    axios({
        url: "http://localhost:5000/api/"+query_id+"/"+H+"/"+T,
        method: 'get',
    }).then(response => {
        console.log(response);
        $('.out').remove()
        if (jQuery.isEmptyObject(response['data'])) {
            predict(query_id, H, T, limit-1)
        }
        else {
            kv = createDictData(response)
			x = kv[0]
			y = kv[1]
            console.log(x)
            console.log(y)
		    renderGraph(query_id, x, y)
		}
    } ).catch(error => {
        console.log(error)
    })
}
	

function renderGraph(query_id, x, y) {
    switch(Number(query_id)) {
		case 1:
			title = "top_protocol_H_T"
			//var ctx = $('#top_protocol_H_T').get(0).getContext('2d')
			//renderPieGraph(ctx, x, y, title)
			break
		case 2:
			title = "top_k_protocols_T"
			ylabel = "count"
			var ctx = $('#top_k_protocols_T').get(0).getContext('2d')
			renderBarGraph(ctx, x, y, ylabel, title)
			break
		case 3:
			title = "protocols_x_more_than_stddev"
			renderBarGraph(x, y, title)
		case 4:
			title = "top_ip_addr_H_T"
			var ctx = $('#top_ip_addr_H_T').get(0).getContext('2d')
			renderPieGraph(ctx, x, y, title)
			break;
		case 5:
			title = "top_k_ip_T"
			renderBarGraph(x, y, title)
			break
		case 6:
			title = "ip_x_more_than_stddev"
			renderBarGraph(x, y, title)
			break
	}
}
	

function createDictData(response) {
    var keys = []
    var values = []
    for (var k in response["data"]){
        keys.push(k)
        values.push(Number(response["data"][k]))
    }
    return [keys, values]
}

function renderBarGraph(ctx, x, y, ylabel, title) {
	
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
            display: true,
            text: title
        }
		}
	});
}

function renderPieGraph(ctx, x, y, ylabel, title) {
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
		display: true,
		text: title	
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

$(".msg").keydown(function () {
    if (event.keyCode == "13") {
        send();
        $(".msg").val(null);
    }
});
