const API = {
    checkReal: `http://localhost:3001/auth`,
    getVisualize: `http://localhost:3001/visualize`
}

function init(data) {
    var neo4jd3 = new Neo4jd3('#neo4jd3', {
        highlight: [
            {
                class: 'Project',
                property: 'name',
                value: 'neo4jd3'
            }, {
                class: 'User',
                property: 'userId',
                value: 'eisman'
            }
        ],
        icons: {
            //                        'Address': 'home',
            'Api': 'gear',
            //                        'BirthDate': 'birthday-cake',
            'Cookie': 'paw',
            //                        'CreditCard': 'credit-card',
            //                        'Device': 'laptop',
            'Email': 'at',
            'Git': 'git',
            'Github': 'github',
            'Google': 'google',
            //                        'icons': 'font-awesome',
            'Ip': 'map-marker',
            'Issues': 'exclamation-circle',
            'Language': 'language',
            'Options': 'sliders',
            'Password': 'lock',
            'Phone': 'phone',
            'Project': 'folder-open',
            'SecurityChallengeAnswer': 'commenting',
            'User': 'user',
            'zoomFit': 'arrows-alt',
            'zoomIn': 'search-plus',
            'zoomOut': 'search-minus'
        },
        images: {
            'Address': 'img/twemoji/1f3e0.svg',
            //                        'Api': 'img/twemoji/1f527.svg',
            'BirthDate': 'img/twemoji/1f382.svg',
            'Cookie': 'img/twemoji/1f36a.svg',
            'CreditCard': 'img/twemoji/1f4b3.svg',
            'Device': 'img/twemoji/1f4bb.svg',
            'Email': 'img/twemoji/2709.svg',
            'Git': 'img/twemoji/1f5c3.svg',
            'Github': 'img/twemoji/1f5c4.svg',
            'icons': 'img/twemoji/1f38f.svg',
            'Ip': 'img/twemoji/1f4cd.svg',
            'Issues': 'img/twemoji/1f4a9.svg',
            'Language': 'img/twemoji/1f1f1-1f1f7.svg',
            'Options': 'img/twemoji/2699.svg',
            'Password': 'img/twemoji/1f511.svg',
            //                        'Phone': 'img/twemoji/1f4de.svg',
            'Project': 'img/twemoji/2198.svg',
            'Project|name|neo4jd3': 'img/twemoji/2196.svg',
            //                        'SecurityChallengeAnswer': 'img/twemoji/1f4ac.svg',
            'User': 'img/twemoji/1f600.svg'
            //                        'zoomFit': 'img/twemoji/2194.svg',
            //                        'zoomIn': 'img/twemoji/1f50d.svg',
            //                        'zoomOut': 'img/twemoji/1f50e.svg'
        },
        minCollision: 60,
        neo4jData: data,
        nodeRadius: 25,
        onNodeDoubleClick: function (node) {
            switch (node.id) {
                case '25':
                    // Google
                    window.open(node.properties.url, '_blank');
                    break;
                default:
                    var maxNodes = 5,
                        data = neo4jd3.randomD3Data(node, maxNodes);
                    neo4jd3.updateWithD3Data(data);
                    break;
            }
        },
        onRelationshipDoubleClick: function (relationship) {
            console.log('double click on relationship: ' + JSON.stringify(relationship));
        },
        zoomFit: true
    });
}
// window.onload = init;

(function (i, s, o, g, r, a, m) {
    i['GoogleAnalyticsObject'] = r; i[r] = i[r] || function () {
        (i[r].q = i[r].q || []).push(arguments)
    }, i[r].l = 1 * new Date(); a = s.createElement(o),
        m = s.getElementsByTagName(o)[0]; a.async = 1; a.src = g; m.parentNode.insertBefore(a, m)
})(window, document, 'script', 'https://www.google-analytics.com/analytics.js', 'ga');

ga('create', 'UA-430863-29', 'auto');
ga('send', 'pageview');

$("footer").html(
    `
          <div class="container my-auto">
            <div class="copyright text-center my-auto">
              <span>BKC TEAM</span>
            </div>
          </div>
          `
)
function handleVisualize() {
    let text = $("#text-search").val()
    var request = $.ajax({
        method: "post",
        url: API.getVisualize,
        data: {
            content: text
        },
        cache: false,
        statusCode: {
            404: function () {
                alert("page not found");
            }
        }
    })
    request.done(function (response) {
        console.log(response);
        if (response && response.data) {
            init(response.data)
        }
    });
    request.fail(function (jqXHR, textStatus) {
        console.log(textStatus);
        alert("Request failed: " + textStatus);
    });
}
$("#button-visual-show").click(() => {
    handleVisualize()
    $("#neo4jd3").show()
    $("#button-visual-hide").show()
    $("#button-visual-show").hide()
})
$("#button-visual-hide").click(() => {
    $("#button-visual-hide").hide()
    $("#button-visual-show").show()
    $("#neo4jd3").hide()
})
function tabRelate(title, content, link, img) {
    return (`
        <div class="col-lg-6 mb-4">
            <div class="card shadow mb-4">
                <div class="card-header py-3">
                    <h6 class="m-0 font-weight-bold text-primary">${title}
                    </h6>
                </div>
                <div class="card-body">
                    <div class="text-center">
                        <img class="img-fluid px-3 px-sm-4 mt-3 mb-4"
                            style="width: 25rem;" src=${img}
                            alt="${title}" />
                    </div>
                    <p>${content}</p>
                    <a target="_blank" rel="nofollow"
                        href=${link}>${link}
                    &rarr;</a>
                </div>
            </div>
        </div>
        `
    )
}
function handleSubmit() {
    let text = $("#text-search").val()
    var request = $.ajax({
        method: "post",
        url: API.checkReal,
        data: {
            content: text
        },
        cache: false,
        statusCode: {
            404: function () {
                alert("page not found");
            }
        }
    })
    request.done(function (response) {
        console.log(response);
        if (response && response.inforRelate) {
            $("#content-relate").html(
                response.inforRelate.map((val, index) => {
                    return tabRelate(val.title, val.content, val.link, val.img)
                })
            )
        }
        if (response.isTrue) {
            $("#isTrue").html(
                `<h1 class="text-success text-center">${response.content}</h1>               
                <h1 >Information Relate</h1>`
            )
        }
        else {
            $("#isTrue").html(
                `<h1 class="text-danger text-center">${response.content}</h1>
                <h1 >Information Relate</h1>`
            )
        }
    });
    request.fail(function (jqXHR, textStatus) {
        console.log(textStatus);
        alert("Request failed: " + textStatus);
    });
}
// code here
$(document).ready(() => {
    // $("#visual-data").html(`<h1>Visualize data</h1>`)
    $("#button-visual-hide").hide();
    $("#button-visual-show").hide();
    $("#form-submit").submit((e) => {
        e.preventDefault()
        handleSubmit()
    })
    handleVisualize()
    $("#button-accept").click(() => handleSubmit())
})