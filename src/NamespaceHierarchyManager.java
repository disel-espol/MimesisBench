package org.apache.hadoop.fs.nnmetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NamespaceHierarchyManager
{
  protected static ArrayList<ArrayList<String>> byDepthDirs = null;
  private static final int maxDepth = 32;
  private boolean hierarchyHasBeenLoaded = false;
  private EventGenerator gen = null;
  private Configuration conf = null;
   
  public NamespaceHierarchyManager(EventGenerator g, Configuration c)
  {
    byDepthDirs = new ArrayList(32);
    this.gen = g;
    this.conf = c;
  }
   
  public String getDirAtDepthForFile(int depth, int file)
  { 
    if (!this.hierarchyHasBeenLoaded) {
      throw new RuntimeException("Cannot call getDirAtDepthForFile() before calling load()");
    }
    int numDirsInTargetDepth = ((ArrayList)byDepthDirs.get(depth)).size();
    if (numDirsInTargetDepth == 0) {
      throw new RuntimeException("ERROR: No dirs in target depth ( " + depth + ") assigned to " + "type " + this.gen.getFileTypeName() + ": " + ((ArrayList)byDepthDirs.get(depth)).toString() + "; dirs assigned to type in depth - 1: " + (depth > 0 ? ((ArrayList)byDepthDirs.get(depth - 1)).toString() : "N/A"));
    }
    return (String)((ArrayList)byDepthDirs.get(depth)).get(file % numDirsInTargetDepth);
  }
  
  public void load()
  {
    String hierarchyFilePath = this.conf.get("test.nnmetadata.conffile", "mimesis.properties") + ".namespace";
    boolean useASingleFileType = this.conf.getBoolean("test.nnmetadata.useASingleFileType", false);
    
    byDepthDirs.add(0, new ArrayList(1));
    for (int i = 1; i < 32; i++) {
      byDepthDirs.add(i, new ArrayList());
    }
    try
    {
      FileSystem fs = FileSystem.get(this.conf);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(hierarchyFilePath))));
      String line;
      while (( line = reader.readLine()) != null)
      {
        int depth = StringUtils.countMatches(line, "/") - 1;
        ((ArrayList)byDepthDirs.get(depth)).add(line);
      }
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    if (((ArrayList)byDepthDirs.get(0)).size() > 1) {
      throw new RuntimeException("ERROR: More than one directory in rooot; " + ((ArrayList)byDepthDirs.get(0)).toString());
    }
    for (ArrayList<String> list : byDepthDirs) {
      if (list.size() >= 3 * this.gen.getNumFileTypes())
      {
        int i = 0;
        int numFileTypes = this.gen.getNumFileTypes();
        int fileType = this.gen.getFileTypeName();
        int mapperID = this.gen.getMapperID();
        for (i = 0; i < numFileTypes; i++) {
          if (i != mapperID) {
            list.set(i, null);
          }
        }
        int firstDirForCurrentType = numFileTypes;
        double weight = 0.0D;
        for (i = 0; i < fileType; i++)
        {
          if (useASingleFileType)
          {
            if (this.gen.getFileTypeWeight(fileType) * numFileTypes > 1.0D) {
              weight = 1 / numFileTypes;
            } else {
              weight = this.gen.getFileTypeWeight(fileType);
            }
          }
          else {
            weight = this.gen.getFileTypeWeight(i);
          }
          firstDirForCurrentType += (int)Math.round(weight * (list.size() - numFileTypes));
        }
        if (firstDirForCurrentType >= list.size()) {
          firstDirForCurrentType = list.size() - 1;
        }
        for (i = numFileTypes; i < firstDirForCurrentType; i++) {
          list.set(i, null);
        }
        if ((useASingleFileType) && (this.gen.getFileTypeWeight(fileType) * numFileTypes > 1.0D)) {
          weight = 1 / numFileTypes;
        } else {
          weight = this.gen.getFileTypeWeight(fileType);
        }
        int firstDirForNextType = firstDirForCurrentType + (int)Math.round(weight * (list.size() - numFileTypes));
        if (firstDirForNextType == firstDirForCurrentType) {
          firstDirForNextType++;
        }
        for (i = firstDirForNextType; i < list.size(); i++) {
          list.set(i, null);
        }
        Iterator<String> it = list.iterator();
        while (it.hasNext()) {
          if (it.next() == null) {
            it.remove();
          }
        }
      }
    }
    this.hierarchyHasBeenLoaded = true;
  }
}
