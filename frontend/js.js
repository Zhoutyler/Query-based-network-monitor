var msg = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
function predict(query_id, H, T) {
    axios({
        url: "http://localhost:5000/api/"+query_id+"/"+H+"/"+T,
        method: 'get',
    }).then(response => {
        console.log(response);
        $('.out').remove()
        addoutput(response['data'])
    } ).catch(error => {
        console.log(error)
    })
}

function addoutput(message) {
    var m = message.split(',')
    var outp = $(".outp")
    var d1 = $("<div class = 'out'> Price:"+ m[0] + "</div>");
    var d2 = $("<div class = 'out'> Deviation:" + m[1] + "</div>");
    outp.append(d1);
    outp.append(d2);
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
function clik() {
    document.getElementById("modal01").style.display = 'none';
}