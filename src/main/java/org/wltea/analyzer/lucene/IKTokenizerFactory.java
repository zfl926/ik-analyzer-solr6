/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wltea.analyzer.lucene;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.wltea.analyzer.dic.Dictionary;

/**
 * @author liangyongxing
 * @editTime 2017-02-06
 * 增加IK扩展词库动态更新类
 */
public class IKTokenizerFactory extends TokenizerFactory
        implements ResourceLoaderAware, UpdateKeeper.UpdateJob {
  private boolean useSmart = false;
  private ResourceLoader loader;
  private long lastUpdateTime = new Date().getTime();
  private String conf = null;

  public IKTokenizerFactory(Map<String, String> args) {
    super(args);
    this.useSmart = getBoolean(args, "useSmart", false);
    this.conf = get(args, "conf");
    System.out.println(String.format(":::ik:construction:::::::::::::::::::::::::: %s", this.conf));
  }

    @Override
    public Tokenizer create(AttributeFactory attributeFactory) {
        return new IKTokenizer(attributeFactory, useSmart());
    }

  @Override
  public void inform(ResourceLoader resourceLoader) throws IOException {
      System.out.println(String.format(":::ik:::inform:::::::::::::::::::::::: %s", this.conf));
      this.loader = resourceLoader;
      if ( loader instanceof ClasspathResourceLoader ){
    	  System.out.println(":::ik:::inform:::::::::::::::::::::::: good!");
      }
      update(true);
      if ((this.conf != null) && (!this.conf.trim().isEmpty())) {
          UpdateKeeper.getInstance().register(this);
      }
  }

  @Override
  /**
   * 执行更新词典操作
   * @throws IOException
   */
  public void update(Boolean isFirst) throws IOException {
      System.out.println("begin into [IKTokenizerFactory.update] method!!! ============================");
      Properties p = null;
      if ( !isFirst )
    	  p = canUpdate();
      else {
    	  p = new Properties();
          InputStream confStream = this.loader.openResource(this.conf);
          p.load(confStream); 
      }
      if (p != null) {
          List<String> dicPaths = SplitFileNames(p.getProperty("files"));
          List<InputStream> inputStreamList = new ArrayList<>();
          for (String path : dicPaths) {
              if ((path != null) && (!path.isEmpty())) {
                  InputStream is = this.loader.openResource(path);

                  if (is != null) {
                	  
                      inputStreamList.add(is);
                  }
              }
          }
          if (!inputStreamList.isEmpty())
              Dictionary.reloadDic(inputStreamList);
      }
  }

    /**
     * 检查是否要更新
     * @return
     */
    private Properties canUpdate() {
        System.out.println("begin into [IKTokenizerFactory.canUpdate] method!!! ");
        try {
            if (this.conf == null){
            	System.out.println("cancel [IKTokenizerFactory.canUpdate] because of the conf is null!!! ");
                return null;
            }
            Properties p = new Properties();
            InputStream confStream = this.loader.openResource(this.conf);
            p.load(confStream);
            confStream.close();
            String timeToUpdate = p.getProperty("lastupdate", "0");
            //System.err.println(String.format("read %s file get lastupdate is %s.", this.conf, lastupdate));
            // we should check the diff from now to lasttime
            Long t = new Date().getTime();
            Long diff = t - this.lastUpdateTime;
            System.out.println("[IKTokenizerFactory.canUpdate] diff time = " + diff + " timeToUpdate = " + timeToUpdate);
            if (diff > Long.valueOf(timeToUpdate)) {
                this.lastUpdateTime = t.longValue();
                String paths = p.getProperty("files");
                if ((paths == null) || (paths.trim().isEmpty()))
                    return null;
                System.out.println("loading conf files success.");
                return p;
            }
            //this.lastUpdateTime = t.longValue();
            return null;
        }
        catch (Exception e) {
            e.printStackTrace();
            //System.err.println("IK parsing conf NullPointerException~~~~~" + e.getStackTrace());
        }
        return null;
    }

    public static List<String> SplitFileNames(String fileNames) {
        if (fileNames == null) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String file : fileNames.split("[,\\s]+")) {
            result.add(file);
        }

        return result;
    }

  private boolean useSmart() {
    return this.useSmart;
  }
}