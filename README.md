# k-means-hadoop
Setup k-means algorithms using Hadoop MapReduce for clustering on 2-dimensional space. <br /><br />
![K-means](https://miro.medium.com/max/1017/1*vNng_oOsNRHKrlh3pjSAyA.png)  <br /><br />
---
### pseudo code about the idea of algorithm
```
     while (not converging)
         map ()
         reduce ()
    
     map ():
         get a list of centers
         Check which center is closest to the point of interest, take the nearest center and record it
   
    reduce ():
        Now we have a list of points assigned to the center
        recalculate the mind and save it
```
