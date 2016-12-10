// $(function() {
//     var map = new Datamap({element: document.getElementById("container"), scope: "usa"});
// });

$(document).ready(function() {
	

	d3.json("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",function(error,data){
        data = data.features;

      	var map = new Datamap({
        	scope: 'usa',
        	element: document.getElementById('container'),
        	fills: {
				defaultFill: '#428bca',
				//lt50: 'red',
				gt50: 'rgba(217,83,79,0.5)',
        	},
	        geographyConfig: {
				highlightOnHover: false,
				popupOnHover: false,
	        },
	        bubblesConfig: {
				highlightFillColor: 'rgba(217,83,79,1)',
	        }
      	});
		var bubble_array = [];

      	function getBubble(lat, lon, name){
	        var bubble = {
	            name: name,
	            latitude: lat,
	            longitude: lon,
	            radius: 10,
	            fillKey: 'gt50',
	        }
        	bubble_array.push(bubble);
        	return bubble;
      	}

		for (var i = 0; i < data.length; i++) {
		  	getBubble(data[i].geometry.coordinates[1],
		    data[i].geometry.coordinates[0],
		    data[i].properties.title);
		}

       //bubbles, custom popup on hover template
     	map.bubbles(bubble_array, {
       		popupTemplate: function(geo, data) {
         		return "<div class='hoverinfo'>" + data.name + "</div>";
       		}
		});
 	});
});