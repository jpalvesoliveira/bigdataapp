angular.module('bigDataApp').controller('BolsaFamiliaController', ['$scope','$http', function($scope,$http) {
	$scope.pesquisar = function (ano, mes){
		console.log(ano);
		console.log(mes);
		
		$scope.dados = [];
		
		$http.get("http://localhost:5260/api/chamado?ano="+ano+"&mes="+mes+"").success(function (data, status){
			console.log("sucesso rest");
			console.log(data);
			drawChartDoacoes();
		});
	};
}]);