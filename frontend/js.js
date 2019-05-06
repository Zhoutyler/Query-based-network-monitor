function predict(query_id, H, T) {
    axios({
        url: "http://localhost:5000/api/"+query_id+"/"+H+"/"+T,
        method: 'get',
    }).then(response => {
        console.log(response);
        $('.out').remove()
        if (jQuery.isEmptyObject(response['data'])) {
            predict(query_id, H, T)
        }
        else
            createDictData(response)
    } ).catch(error => {
        console.log(error)
    })
}



function createDictData(response) {
    var keys = []
    var values = []
	for (var k in response["data"]){
		keys.push(k)
		values.push(Number(response["data"][k]))
	}
    console.log(keys)
	console.log(values)

    //var keys = [1, 2]
    //var values = [3, 4]
    renderGraph(keys, values)
}

function renderGraph(x, y) {
	var ctx = $('#myChart').get(0).getContext('2d');
		var myChart = new Chart(ctx, {
		    type: 'bar',
		    data: {
			labels: x,
			datasets: [{
			    label: '# of Votes',
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
