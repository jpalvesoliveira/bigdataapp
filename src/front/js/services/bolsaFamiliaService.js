angular.module('bigDataApp').factory('bolsaFamiliaService', function () {
	var _drawChart = function drawChart() {
		var data = google.visualization.arrayToDataTable([
			['Task', 'Hours per Day'],
			['Work',     11],
			['Eat',      2],
			['Commute',  2],
			['Watch TV', 2],
			['Sleep',    7]
		]);

		var options = {
			title: 'Bolsa Fam�lia',
			 is3D: true,
		};

		var chart = new google.visualization.PieChart(document.getElementById('piechart_3d'));
		chart.draw(data, options);
	}
	
	return {
		drawChart: _drawChart
	};
});