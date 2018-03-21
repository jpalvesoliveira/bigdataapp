angular.module('bigDataApp').controller('BolsaFamiliaController', function ($scope) {
	$scope.pesquisar = function (ano, mes){
		console.log(ano);
		console.log(mes);
		$scope.dados = [
		  ['Par | Ímpar', '%'],
		  ['6P 0I', 1.186],
          ['5P 1I', 8.539],
          ['4P 2I', 23.812],
          ['3P 3I', 32.925],
          ['2P 4I', 23.812],
          ['1P 5I', 8.539],
		  ['0P 6I', 1.186]
		];
		console.log($scope.dados);
	};
});