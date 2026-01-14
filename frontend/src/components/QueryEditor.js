import React, { useState } from 'react';
import {
  Paper,
  TextField,
  Button,
  Typography,
  Box,
  CircularProgress,
  Alert,
  Chip,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { validateQuery, saveQueryToHistory } from '../utils/queryValidator';

const QueryEditor = ({ onExecute, loading }) => {
  const [query, setQuery] = useState('');
  const [validation, setValidation] = useState({ isValid: true, errors: [], warnings: [] });

  const handleQueryChange = (e) => {
    const newQuery = e.target.value;
    setQuery(newQuery);
    
    if (newQuery.trim()) {
      const result = validateQuery(newQuery);
      setValidation(result);
    } else {
      setValidation({ isValid: true, errors: [], warnings: [] });
    }
  };

  const handleExecute = () => {
    const result = validateQuery(query);
    setValidation(result);

    if (result.isValid) {
      saveQueryToHistory(query);
      onExecute(query);
    }
  };

  const handleKeyPress = (e) => {
    if (e.ctrlKey && e.key === 'Enter') {
      handleExecute();
    }
  };

  const characterCount = query.length;
  const characterLimit = 10000;

  return (
    <Paper elevation={3} sx={{ p: 3 }}>
      <Typography variant="h6" gutterBottom fontWeight="bold">
        Custom SQL Query
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Write your SELECT query below. Press Ctrl+Enter to execute quickly.
      </Typography>

      {validation.errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {validation.errors.map((error, index) => (
            <div key={index}>{error}</div>
          ))}
        </Alert>
      )}

      {validation.warnings.length > 0 && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          {validation.warnings.map((warning, index) => (
            <div key={index}>{warning}</div>
          ))}
        </Alert>
      )}

      <TextField
        fullWidth
        multiline
        minRows={8}
        maxRows={20}
        value={query}
        onChange={handleQueryChange}
        onKeyDown={handleKeyPress}
        placeholder="Enter your SELECT query here...&#10;&#10;Example:&#10;SELECT TOP 10 * FROM Dim_Airline&#10;ORDER BY carrier_name"
        variant="outlined"
        sx={{
          mb: 2,
          fontFamily: 'monospace',
          '& .MuiInputBase-input': {
            fontFamily: 'monospace',
            fontSize: '14px',
          },
        }}
      />

      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Chip
            label={`${characterCount} / ${characterLimit} characters`}
            size="small"
            color={characterCount > characterLimit * 0.9 ? 'warning' : 'default'}
          />
        </Box>
        <Button
          variant="contained"
          color="primary"
          size="large"
          startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <PlayArrowIcon />}
          onClick={handleExecute}
          disabled={loading || !query.trim() || !validation.isValid}
          sx={{ minWidth: 160 }}
        >
          {loading ? 'Executing...' : 'Execute Query'}
        </Button>
      </Box>

      <Box sx={{ mt: 2 }}>
        <Typography variant="caption" color="text.secondary">
          💡 <strong>Tip:</strong> Use TOP N to limit results for better performance. Example: SELECT TOP 100 * FROM Fact_Delays
        </Typography>
      </Box>
    </Paper>
  );
};

export default QueryEditor;