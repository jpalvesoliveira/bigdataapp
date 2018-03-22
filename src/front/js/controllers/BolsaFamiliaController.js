angular.module('bigDataApp').controller('bolsaFamiliaController', ['$scope','$http', function($scope,$http) {
	$scope.pesquisar = function (ano, mes){
		var url = "http://localhost:8080/consultar?ano="+ano+"&mes="+mes;
		$http.get(url).then(function (data, status){
			console.log(data);
			var dados = [];
			dados.push(['Tipo', 'Qtd']);
			jQuery.each(data.data, function() {
			  dados.push([ this.natureza, this.total ]);
			});
			drawChartDoacoes(dados);
		});
	};
}]);