import React, { useState, useEffect } from 'react';
import {
  ThemeProvider,
  CssBaseline,
  AppBar,
  Toolbar,
  Typography,
  Container,
  Box,
  IconButton,
  LinearProgress,
  Snackbar,
  Alert,
  Tabs,
  Tab,
  Paper,
  Button,
  Grid,
  Chip,
  CircularProgress,
} from '@mui/material';
import Brightness4Icon from '@mui/icons-material/Brightness4';
import Brightness7Icon from '@mui/icons-material/Brightness7';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import TableRowsIcon from '@mui/icons-material/TableRows';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import QueryEditor from './components/QueryEditor';
import PredefinedQueries from './components/PredefinedQueries';
import ResultsTable from './components/ResultsTable';
import { apiService } from './services/api';
import { getTheme } from './theme';
import tableauScreenshot from './Tableau_Dashboard.png';

// Tableau embed component
function TableauEmbed() {
  const tableauUrl = 'https://public.tableau.com/app/profile/jill.patel4309/viz/Flight_Performance_Dashboard_2024/FlightPerformanceAnalytics';

  const openInNewWindow = () => {
    window.open(tableauUrl, '_blank', 'noopener,noreferrer');
  };

  return (
    <Box sx={{ width: '100%' }}>
      <Paper elevation={3} sx={{ p: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Typography variant="h6">Tableau Dashboard</Typography>
          <Button
            variant="contained"
            color="primary"
            startIcon={<OpenInNewIcon />}
            onClick={openInNewWindow}
          >
            Open in new window
          </Button>
        </Box>
        <Box sx={{ width: '100%', overflow: 'hidden', display: 'flex', justifyContent: 'center' }}>
          <img
            src={tableauScreenshot}
            alt="Tableau Dashboard"
            style={{ width: '100%', height: 800, objectFit: 'cover', borderRadius: 4 }}
          />
        </Box>
      </Paper>
    </Box>
  );
}

function App() {
  const [darkMode, setDarkMode] = useState(false);
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(true);
  const [predefinedQueries, setPredefinedQueries] = useState([]);
  const [tabValue, setTabValue] = useState(0); // 0 = Predefined, 1 = Custom, 2 = Tableau
  // state variable to explicitly track whether the Tableau tab is active (tabValue === 2)
  const [isTableauActive, setIsTableauActive] = useState(false);
  const [comparisonResults, setComparisonResults] = useState(null);
  const [warehouseLoading, setWarehouseLoading] = useState(false);
  const [normalizedLoading, setNormalizedLoading] = useState(false);
  const [error, setError] = useState(null);
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: '',
    severity: 'info',
  });

  const theme = getTheme(darkMode ? 'dark' : 'light');

  // Fetch database stats on mount
  useEffect(() => {
    fetchDatabaseStats();
    fetchPredefinedQueries();
  }, []);

  // keep `isTableauActive` in sync with the selected tab
  useEffect(() => {
    setIsTableauActive(tabValue === 2);
  }, [tabValue]);

  const fetchDatabaseStats = async () => {
    setStatsLoading(true);
    try {
      const response = await apiService.getDatabaseMetrics();
      console.log('Stats response:', response); // Debug log

      // Handle both direct response and nested data
      if (response.metrics) {
        setStats(response.metrics);
      } else if (response.data && response.data.metrics) {
        setStats(response.data.metrics);
      } else {
        setStats(response);
      }
    } catch (error) {
      console.error('Error fetching database stats:', error);
      showSnackbar('Failed to fetch database statistics', 'error');
      // Fallback to mock data
      setStats({
        total_flights: 7079081,
        speed_up_factor: 3.19,
        data_quality: 98.62,
        query_response: 3.05,
        avg_delay_minutes: 7.39,
        on_time_percentage: 63.65,
      });
    } finally {
      setStatsLoading(false);
    }
  };

  const fetchPredefinedQueries = async () => {
    try {
      const data = await apiService.getPredefinedQueries();
      setPredefinedQueries(data.queries || []);
    } catch (error) {
      console.error('Error fetching predefined queries:', error);
      showSnackbar('Failed to fetch predefined queries', 'error');
    }
  };

  const handleExecuteCustomQuery = async (query) => {
    handleComparisonQuery(query);
  };

  const handleExecutePredefinedQuery = async (queryId) => {
    // Get the query SQL from predefined queries
    const query = predefinedQueries.find(q => q.id === queryId);
    if (query) {
      // Pass both query ID (for mock mode) and SQL (for real API calls)
      handleComparisonQuery(query.sql, queryId);
    }
  };

  const handleComparisonQuery = async (query, queryId = null) => {
    setWarehouseLoading(true);
    setNormalizedLoading(true);
    setError(null);
    setComparisonResults(null);

    // Initialize results structure
    let warehouseData = null;
    let normalizedData = null;

    // Pass both queryId (for mock mode) and SQL (for real API calls)
    const queryParam = queryId !== null ? { query, queryId } : query;

    // Make parallel API calls to both databases
    const warehousePromise = apiService.executeWarehouseQuery(queryParam)
      .then((result) => {
        warehouseData = result;
        setWarehouseLoading(false);
        return result;
      })
      .catch((err) => {
        setWarehouseLoading(false);
        throw new Error(`Warehouse query failed: ${err.message || err}`);
      });

    const normalizedPromise = apiService.executeNormalizedQuery(queryParam)
      .then((result) => {
        normalizedData = result;
        setNormalizedLoading(false);
        return result;
      })
      .catch((err) => {
        setNormalizedLoading(false);
        throw new Error(`Normalized query failed: ${err.message || err}`);
      });

    try {
      // Wait for both queries to complete
      await Promise.all([warehousePromise, normalizedPromise]);

      // Calculate comparison metrics
      const warehouseTime = warehouseData.execution_time_ms;
      const normalizedTime = normalizedData.execution_time_ms;
      const speedup = warehouseTime > 0 ? (normalizedTime / warehouseTime).toFixed(2) : 1.0;
      const improvementPct = normalizedTime > 0
        ? (((normalizedTime - warehouseTime) / normalizedTime) * 100).toFixed(1)
        : 0.0;
      const timeSaved = (normalizedTime - warehouseTime).toFixed(2);

      // Update results
      const finalResults = {
        warehouse: {
          data: warehouseData.data,
          execution_time_ms: warehouseData.execution_time_ms,
          row_count: warehouseData.row_count,
          columns: warehouseData.columns,
        },
        normalized: {
          data: normalizedData.data,
          execution_time_ms: normalizedData.execution_time_ms,
          row_count: normalizedData.row_count,
          columns: normalizedData.columns,
        },
        comparison: {
          speedup: parseFloat(speedup),
          improvement_pct: parseFloat(improvementPct),
          time_saved_ms: parseFloat(timeSaved),
        },
      };

      setComparisonResults(finalResults);
      setSnackbar({
        open: true,
        message: `Comparison complete! Warehouse is ${speedup}× faster`,
        severity: 'success',
      });
    } catch (err) {
      setError(err);
      setSnackbar({
        open: true,
        message: err.message || 'Failed to execute comparison query',
        severity: 'error',
      });
    }
  };

  const showSnackbar = (message, severity = 'info') => {
    setSnackbar({
      open: true,
      message,
      severity,
    });
  };

  const handleCloseSnackbar = () => {
    setSnackbar({ ...snackbar, open: false });
  };

  const toggleDarkMode = () => {
    setDarkMode(!darkMode);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ flexGrow: 1, minHeight: '100vh' }}>
        {/* App Bar */}
        <AppBar position="static" elevation={2}>
          <Toolbar>
            <Box sx={{ flexGrow: 1, textAlign: 'center', mt: '10px', ml: '10px', mr: '10px' }}>
              <Typography variant="h3" component="div" fontWeight="bold" sx={{ mb: 0.5 }}>
                Flight Data Warehouse - Query Interface
              </Typography>
              <Typography variant="h6" sx={{ opacity: 0.9 }}>
                2024 US Domestic Flights Analysis
              </Typography>
            </Box>
            <IconButton onClick={toggleDarkMode} color="inherit" sx={{ position: 'absolute', right: 16 }}>
              {darkMode ? <Brightness7Icon /> : <Brightness4Icon />}
            </IconButton>
          </Toolbar>
        </AppBar>

        {/* Loading Progress */}
        {loading && <LinearProgress />}

        {/* Main Content */}
        <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
          {/* Stats Cards - 3 Cards Only, Horizontal Layout */}
          <Box sx={{ mb: 4 }}>
            <Grid container spacing={3}>

              {/* Total Flights */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'primary.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>🛫</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : stats?.total_flights?.toLocaleString() || '0'}
                    </Typography>
                    <Typography variant="body1">Records Processed in total</Typography>
                  </Box>
                </Box>
              </Grid>

              {/* Speed Up Factor */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'secondary.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>⚡</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : stats?.speed_up_factor || '0'} x
                    </Typography>
                    <Typography variant="body1">Performance Improvement by warehouse</Typography>
                  </Box>
                </Box>
              </Grid>

              {/* Data Quality */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'error.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>📊</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : (stats?.data_quality != null ? `${stats.data_quality}` : '0')} %
                    </Typography>
                    <Typography variant="body1">Data Quality after ETL</Typography>
                  </Box>
                </Box>
              </Grid>

              {/* Query Response Time */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'warning.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>⌛</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : `${(stats?.query_response || 0).toLocaleString()}`} s
                    </Typography>
                    <Typography variant="body1">Query response time</Typography>
                  </Box>
                </Box>
              </Grid>

              {/* Average Delay */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'info.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>⏱️</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : stats?.avg_delay_minutes?.toFixed(2) || '0.00'} min
                    </Typography>
                    <Typography variant="body1">Average Delay</Typography>
                  </Box>
                </Box>
              </Grid>

              {/* On-time Percentage */}
              <Grid item xs={12} md={4}>
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    p: 3,
                    bgcolor: 'success.main',
                    borderRadius: 2,
                    color: 'white',
                  }}
                >
                  <Box sx={{ mr: 3, fontSize: 48 }}>👍</Box>
                  <Box>
                    <Typography variant="h3" sx={{ fontWeight: 'bold' }}>
                      {statsLoading ? '...' : (stats?.on_time_percentage != null ? `${stats.on_time_percentage}` : '0')} %
                    </Typography>
                    <Typography variant="body1">On-time Flight Rate</Typography>
                  </Box>
                </Box>
              </Grid>

            </Grid>
          </Box>

          {/* Tabbed Query Interface */}
          <Box sx={{ mb: 4 }}>
            <Tabs
              value={tabValue}
              onChange={(e, newValue) => setTabValue(newValue)}
              sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}
            >
              <Tab label="Predefined Queries" />
              <Tab label="Custom SQL Query" />
              <Tab label="Tableau Dashboard" />
            </Tabs>

            {/* Tab 1: Predefined Queries */}
            {tabValue === 0 && (
              <PredefinedQueries
                queries={predefinedQueries}
                onExecute={handleExecutePredefinedQuery}
                loading={warehouseLoading || normalizedLoading}
              />
            )}

            {/* Tab 2: Custom SQL Query */}
            {tabValue === 1 && (
              <QueryEditor
                onExecute={handleComparisonQuery}
                loading={warehouseLoading || normalizedLoading}
              />
            )}

            {/* Tab 3: Tableau Dashboard */}
            {tabValue === 2 && (
              <TableauEmbed />
            )}
          </Box>

          {/* Comparison Results - Side by Side */}
          {(comparisonResults || warehouseLoading || normalizedLoading) && (
            <Box sx={{ mb: 4 }}>
              <Typography variant="h5" gutterBottom>
                Query Results - Database Comparison
              </Typography>

              {/* Performance Comparison Banner - Only show when both results are available */}
              {comparisonResults && comparisonResults.comparison && (
                <Box
                  sx={{
                    p: 2,
                    mb: 3,
                    bgcolor: 'success.light',
                    borderRadius: 2,
                    display: 'flex',
                    justifyContent: 'space-around',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 2,
                  }}
                >
                  <Typography variant="h6">
                    ⚡ Speedup: <strong>{comparisonResults.comparison.speedup}×</strong>
                  </Typography>
                  <Typography variant="h6">
                    📈 Improvement: <strong>{comparisonResults.comparison.improvement_pct}%</strong>
                  </Typography>
                  <Typography variant="h6">
                    ⏱️ Time Saved: <strong>{comparisonResults.comparison.time_saved_ms.toFixed(0)} ms</strong>
                  </Typography>
                </Box>
              )}

              <Grid container spacing={2}>
                {/* Left: Warehouse Results */}
                <Grid item xs={12} md={6}>
                  <Box sx={{ border: 2, borderColor: 'success.main', borderRadius: 2, p: 2, maxHeight: '80vh', display: 'flex', flexDirection: 'column' }}>
                    <Typography variant="h6" gutterBottom color="success.main">
                      ⚡ Data Warehouse (Star Schema)
                    </Typography>
                    {comparisonResults && comparisonResults.warehouse && (
                      <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
                        <Chip
                          icon={<AccessTimeIcon />}
                          label={`${comparisonResults.warehouse.execution_time_ms} ms`}
                          color="primary"
                        />
                        <Chip
                          icon={<TableRowsIcon />}
                          label={`${comparisonResults.warehouse.row_count} rows`}
                          color="secondary"
                        />
                        <Chip icon={<CheckCircleIcon />} label="Success" color="success" />
                      </Box>
                    )}
                    <Box sx={{ flex: 1, overflow: 'auto' }}>
                      {warehouseLoading ? (
                        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                          <CircularProgress />
                        </Box>
                      ) : comparisonResults && comparisonResults.warehouse ? (
                        <ResultsTable
                          results={comparisonResults.warehouse.data}
                          queryMetrics={{
                            execution_time_ms: comparisonResults.warehouse.execution_time_ms,
                            row_count: comparisonResults.warehouse.row_count,
                            columns: comparisonResults.warehouse.columns,
                          }}
                        />
                      ) : null}
                    </Box>
                  </Box>
                </Grid>

                {/* Right: Normalized Results */}
                <Grid item xs={12} md={6}>
                  <Box sx={{ border: 2, borderColor: 'warning.main', borderRadius: 2, p: 2, maxHeight: '80vh', display: 'flex', flexDirection: 'column' }}>
                    <Typography variant="h6" gutterBottom color="warning.main">
                      🐌 Normalized Database (3NF)
                    </Typography>
                    {comparisonResults && comparisonResults.normalized && (
                      <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
                        <Chip
                          icon={<AccessTimeIcon />}
                          label={`${comparisonResults.normalized.execution_time_ms} ms`}
                          color="primary"
                        />
                        <Chip
                          icon={<TableRowsIcon />}
                          label={`${comparisonResults.normalized.row_count} rows`}
                          color="secondary"
                        />
                        <Chip icon={<CheckCircleIcon />} label="Success" color="success" />
                      </Box>
                    )}
                    <Box sx={{ flex: 1, overflow: 'auto' }}>
                      {normalizedLoading ? (
                        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                          <CircularProgress />
                        </Box>
                      ) : comparisonResults && comparisonResults.normalized ? (
                        <ResultsTable
                          results={comparisonResults.normalized.data}
                          queryMetrics={{
                            execution_time_ms: comparisonResults.normalized.execution_time_ms,
                            row_count: comparisonResults.normalized.row_count,
                            columns: comparisonResults.normalized.columns,
                          }}
                        />
                      ) : null}
                    </Box>
                  </Box>
                </Grid>
              </Grid>
            </Box>
          )}
        </Container>

        {/* Snackbar for notifications */}
        <Snackbar
          open={snackbar.open}
          autoHideDuration={6000}
          onClose={handleCloseSnackbar}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        >
          <Alert
            onClose={handleCloseSnackbar}
            severity={snackbar.severity}
            variant="filled"
            sx={{ width: '100%' }}
          >
            {snackbar.message}
          </Alert>
        </Snackbar>
      </Box>
    </ThemeProvider>
  );
}

export default App;
