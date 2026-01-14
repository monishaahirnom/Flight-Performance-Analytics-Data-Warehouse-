import React, { useState } from 'react';
import {
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  CardActions,
  Button,
  Box,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import CodeIcon from '@mui/icons-material/Code';

const PredefinedQueries = ({ queries, onExecute, loading }) => {
  const [expandedQuery, setExpandedQuery] = useState(null);

  const handleExpandClick = (queryId) => {
    setExpandedQuery(expandedQuery === queryId ? null : queryId);
  };

  return (
    <Paper elevation={3} sx={{ p: 3 }}>
      <Typography variant="h6" gutterBottom fontWeight="bold">
        Predefined Analytical Queries
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Select from pre-built queries designed to showcase the data warehouse capabilities
      </Typography>

      <Grid container spacing={2}>
        {queries.map((query) => (
          <Grid item xs={12} md={6} key={query.id}>
            <Card 
              elevation={2}
              sx={{ 
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                transition: 'transform 0.2s',
                '&:hover': {
                  transform: 'translateY(-4px)',
                  boxShadow: 4,
                },
              }}
            >
              <CardContent sx={{ flexGrow: 1 }}>
                <Typography variant="h6" component="div" gutterBottom fontWeight="bold">
                  {query.name}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {query.description}
                </Typography>
              </CardContent>
              
              <CardActions sx={{ justifyContent: 'space-between', px: 2, pb: 2 }}>
                <Button
                  size="small"
                  startIcon={<CodeIcon />}
                  onClick={() => handleExpandClick(query.id)}
                >
                  {expandedQuery === query.id ? 'Hide SQL' : 'View SQL'}
                </Button>
                <Button
                  variant="contained"
                  size="small"
                  startIcon={<PlayArrowIcon />}
                  onClick={() => onExecute(query.id)}
                  disabled={loading}
                >
                  Run Query
                </Button>
              </CardActions>

              {expandedQuery === query.id && (
                <Box sx={{ px: 2, pb: 2 }}>
                  <Paper 
                    elevation={0} 
                    sx={{ 
                      p: 2, 
                      backgroundColor: 'grey.100',
                      maxHeight: 300,
                      overflow: 'auto',
                    }}
                  >
                    <Typography
                      component="pre"
                      variant="body2"
                      sx={{
                        fontFamily: 'monospace',
                        fontSize: '12px',
                        margin: 0,
                        whiteSpace: 'pre-wrap',
                        wordWrap: 'break-word',
                      }}
                    >
                      {query.sql}
                    </Typography>
                  </Paper>
                </Box>
              )}
            </Card>
          </Grid>
        ))}
      </Grid>
    </Paper>
  );
};

export default PredefinedQueries;