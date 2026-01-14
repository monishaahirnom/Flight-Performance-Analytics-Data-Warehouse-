import React from 'react';
import { Card, CardContent, Typography, Grid, Box, Skeleton } from '@mui/material';
import FlightIcon from '@mui/icons-material/Flight';
import AirplanemodeActiveIcon from '@mui/icons-material/AirplanemodeActive';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CancelIcon from '@mui/icons-material/Cancel';

const StatsCards = ({ stats, loading }) => {
  const cards = [
    {
      title: 'Total Flights',
      value: stats?.total_flights || '0',
      icon: <FlightIcon sx={{ fontSize: 50 }} />,
      color: '#1976d2',
      bgColor: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
      iconBg: 'rgba(255, 255, 255, 0.2)',
    },
    {
      title: 'Total Airports',
      value: stats?.total_airports || '0',
      icon: <AirplanemodeActiveIcon sx={{ fontSize: 50 }} />,
      color: '#2e7d32',
      bgColor: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)',
      iconBg: 'rgba(255, 255, 255, 0.2)',
    },
    {
      title: 'Avg Delay (min)',
      value: stats?.avg_delay ? parseFloat(stats.avg_delay).toFixed(2) : '0.00',
      icon: <AccessTimeIcon sx={{ fontSize: 50 }} />,
      color: '#ed6c02',
      bgColor: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)',
      iconBg: 'rgba(255, 255, 255, 0.2)',
    },
    {
      title: 'Total Cancelled',
      value: stats?.total_cancelled || '0',
      icon: <CancelIcon sx={{ fontSize: 50 }} />,
      color: '#d32f2f',
      bgColor: 'linear-gradient(135deg, #fa709a 0%, #fee140 100%)',
      iconBg: 'rgba(255, 255, 255, 0.2)',
    },
  ];

  return (
    <Grid container spacing={3}>
      {cards.map((card, index) => (
        <Grid item xs={12} sm={6} md={3} key={index}>
          <Card 
            elevation={8}
            sx={{
              background: card.bgColor,
              color: 'white',
              height: '100%',
              transition: 'transform 0.3s ease-in-out, box-shadow 0.3s ease-in-out',
              '&:hover': {
                transform: 'translateY(-8px)',
                boxShadow: '0 12px 24px rgba(0,0,0,0.2)',
              },
            }}
          >
            <CardContent sx={{ position: 'relative', overflow: 'hidden' }}>
              {loading ? (
                <>
                  <Skeleton variant="rectangular" height={60} sx={{ mb: 2, bgcolor: 'rgba(255,255,255,0.1)' }} />
                  <Skeleton variant="text" sx={{ bgcolor: 'rgba(255,255,255,0.1)' }} />
                  <Skeleton variant="text" width="60%" sx={{ bgcolor: 'rgba(255,255,255,0.1)' }} />
                </>
              ) : (
                <>
                  <Box
                    sx={{
                      position: 'absolute',
                      top: -20,
                      right: -20,
                      opacity: 0.15,
                    }}
                  >
                    {React.cloneElement(card.icon, { sx: { fontSize: 120 } })}
                  </Box>
                  
                  <Box
                    sx={{
                      backgroundColor: card.iconBg,
                      borderRadius: 3,
                      p: 2,
                      display: 'inline-flex',
                      mb: 2,
                      backdropFilter: 'blur(10px)',
                    }}
                  >
                    {card.icon}
                  </Box>
                  
                  <Typography 
                    variant="h3" 
                    component="div" 
                    fontWeight="bold" 
                    sx={{ 
                      mb: 1,
                      textShadow: '2px 2px 4px rgba(0,0,0,0.2)',
                    }}
                  >
                    {card.value.toLocaleString()}
                  </Typography>
                  
                  <Typography 
                    variant="body1" 
                    sx={{ 
                      opacity: 0.95,
                      fontWeight: 500,
                      letterSpacing: '0.5px',
                    }}
                  >
                    {card.title}
                  </Typography>
                </>
              )}
            </CardContent>
          </Card>
        </Grid>
      ))}
    </Grid>
  );
};

export default StatsCards;