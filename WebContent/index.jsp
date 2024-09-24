<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!doctype html>
<html lang="en" ng-app="BILLD">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Cache-Control" content="no-store">
  <meta http-equiv='refresh' content='30'>

	<title>CUK Billing Dashboard</title>
  <script src='js/jquery.min.js'></script>
  <link rel="stylesheet" href="css/w3.css">  <link rel='stylesheet' href='css/bootstrap.min.css'>
  <link rel='stylesheet' href='css/bootstrap-theme.min.css'>
  <script src='js/bootstrap.min.js'></script>
  <!-- STYLES -->
  <link rel="stylesheet" href="css/main.min.css"/>
  <!-- SCRIPTS -->
  <script src="js/main.min.js"></script>
  <!-- Custom Scripts -->
  <script type="text/javascript" src="js/dashboard.min.js"></script>
</head>
<body ng-controller="MasterCtrl">
  <div id="page-wrapper" ng-class="{'open': toggle}" ng-cloak>

    <!-- Sidebar -->
    <div id="sidebar-wrapper">
      <ul class="sidebar">
        <li class="sidebar-main">
          <a ng-click="toggleSidebar()">
            Dashboard
            <span class="menu-icon glyphicon glyphicon-transfer"></span>
          </a>
        </li>
        <li class="sidebar-title"><span>NAVIGATION</span></li>
        <li class="sidebar-list">
          <a ng-href="#/dashboard">Dashboard <span class="menu-icon glyphicon glyphicon-dashboard"></span></a>
        </li>
        <li class="sidebar-list">
          <a ng-href="#/tasks">Tasks <span class="menu-icon glyphicon glyphicon-list-alt"></span></a>
        </li>
        <li class="sidebar-list">
          <a ng-href="#/running">Running <span class="menu-icon glyphicon glyphicon-play-circle"></span></a>
        </li>
        <li class="sidebar-list">
          <a ng-href="#/bdbr">BDBR <span class="menu-icon glyphicon glyphicon-transfer"></span></a>
        </li>
      </ul>
    </div>
    <!-- End Sidebar -->

    <div id="content-wrapper">
      <div class="page-content">

        <!-- Header Bar -->     
        
        <div class="row header">
          <div class="col-xs-12">
            <div class="user pull-right">
              <div class="item dropdown">
                  <img src="img/ibm-logo-black.gif">
              </div>
              <div class="item dropdown">
              	<jsp:include page="alerts.html" />
              </div>
            </div>
            <div class="meta">
              <div class="page">
                CUK Billing Dashboard
              </div>
              
            </div>
          </div>
        </div>
        <!-- End Header Bar -->

        
        <!-- End Header Bar -->

        <!-- Main Content -->
        <div ui-view></div>

      </div><!-- End Page Content -->
    </div><!-- End Content Wrapper -->
  </div><!-- End Page Wrapper -->
</body>
</html>



