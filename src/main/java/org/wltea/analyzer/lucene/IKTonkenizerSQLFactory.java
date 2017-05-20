package org.wltea.analyzer.lucene;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.wltea.analyzer.cfg.DefaultConfig;
import org.wltea.analyzer.dic.Dictionary;
import org.wltea.analyzer.util.DBManager;

public class IKTonkenizerSQLFactory extends TokenizerFactory
							implements ResourceLoaderAware {

	private boolean useSmart =false;
	private String conf = null;
	private List<String> extWords = new ArrayList<>();
	private List<String> stopWords = new ArrayList<>();
	private ResourceLoader loader;
	//private boolean isFromDBorFile = false;
	
	private static final String SQL_QUERY = "select type, word from ext_stop_word where is_delete = 0";
	
	private DBManager dbManager;
	
	private Lock lock = new ReentrantLock();
	private static Boolean hasRun = false;
		
	public IKTonkenizerSQLFactory(Map<String, String> args) {
		super(args);
		this.useSmart = getBoolean(args, "useSmart", false);
		this.conf = get(args, "conf");
		// load conf file to connect to db
	    System.out.println(":::ik:construction::::::::::::::::::::::::::");
	}

	private boolean useSmart(){
		return useSmart;
	}
	
	/**
	 * 取得数据库和目前内存的数据中不同的数据
	 * @return
	 */
	private List<String> findDiff(List<String> q1, List<String> q2){
		List<String> diff = new ArrayList<>();
		q1.forEach(e ->{
			if (!q2.contains(e)){
				diff.add(e);
			}
		});
		
		return diff;
	}
		
	public static class Word {
		
		public int type;
		public String word;
		
	}
	
	
	/**
	 * 从数据库初始化词典
	 */
	@Override
	public void inform(ResourceLoader loader) throws IOException {
		System.out.println(":::[inform]::::::::::::::::::::::::::");
		this.loader = loader;
		InputStream confStream = this.loader.openResource(this.conf);
		Properties p = new Properties();
        p.load(confStream);
        
//        System.out.println(Thread.currentThread().getName() + ":::[inform]:::::::::::::::::::::::::: driver = " + p.getProperty("driver"));
//        System.out.println(Thread.currentThread().getName() + ":::[inform]:::::::::::::::::::::::::: url = " + p.getProperty("url"));
//        System.out.println(Thread.currentThread().getName() + ":::[inform]:::::::::::::::::::::::::: driver = " + p.getProperty("user"));
//        System.out.println(Thread.currentThread().getName() + ":::[inform]:::::::::::::::::::::::::: driver = " + p.getProperty("password"));
        
		try {
			
			dbManager = DBManager.getInstance(p.getProperty("driver"),
					p.getProperty("url"),
					p.getProperty("user"),
					p.getProperty("password"));
			// load data from 
			List<Word> words = dbManager.query(SQL_QUERY, rs ->{
				Word word = new Word();
				try {
					word.type = rs.getInt(1);
					word.word = rs.getString(2);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				return word;
			});
			
			words.forEach(word -> {
				if ( word.type == 1 ){
					if ( !extWords.contains(word.word) )
						extWords.add(word.word);
				} else if ( word.type == 2 ){
					if ( !stopWords.contains(word.word) )
						stopWords.add(word.word);
				}
			});
			
			Dictionary sington = null;
			try {
				sington = Dictionary.getSingleton();
			} catch (Exception e){
				sington = Dictionary.initial(DefaultConfig.getInstance());
			}
			
//			System.out.println(Thread.currentThread().getName() + "::[first create] extWords = " + extWords);
//			System.out.println(Thread.currentThread().getName() + "::[first create] stopWords = " + stopWords);
			
			//if ( !hasRun )
			sington.addExtStopDicts(extWords, stopWords);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		try {
			if ( lock.tryLock() ){
				if ( !hasRun ) {
					new Timer(true).schedule(new TimerTask(){
						@Override
						public void run() {
							System.out.println(":::::::::::::::::::[run job]");
							List<Word> words = new ArrayList<>();
							try {
								words = dbManager.query(SQL_QUERY, rs ->{
									Word word = new Word();
									try {
										word.type = rs.getInt(1);
										word.word = rs.getString(2);
									} catch (Exception e) {
										e.printStackTrace();
									}
									
									return word;
								});
								
								List<String> extNew = new ArrayList<>();
								List<String> stopNew = new ArrayList<>();
								
								words.forEach(word -> {
									if ( word.type == 1 ){
										extNew.add(word.word);
									} else if ( word.type == 2 ){
										stopNew.add(word.word);
									}
								});
								
//								System.out.println(Thread.currentThread().getName() + "::[Job]::: load from db words = " + extNew);
//								System.out.println(Thread.currentThread().getName() + "::[Job]::: origin words = " + extWords);
								
								
								List<String> newExtWords = findDiff(extNew, extWords);
								List<String> delExtWords = findDiff(extWords, extNew);
								
								
								
								Dictionary.getSingleton().addWords(newExtWords);
								Dictionary.getSingleton().disableWords(delExtWords);
								
								List<String> newStopWords = findDiff(stopNew, stopWords);
								List<String> delStopWords = findDiff(stopWords, stopNew);
								
//								System.out.println(Thread.currentThread().getName() + "::[Job]::: newExtWords  = " + newExtWords);
//								System.out.println(Thread.currentThread().getName() + "::[Job]::: delExtWords  = " + delExtWords);
								
								Dictionary.getSingleton().addStopWords(newStopWords);
								Dictionary.getSingleton().disableStopWords(delStopWords);
								
							} catch (Throwable e) {
								e.printStackTrace();
							}
						}
					}, 15000, 3000);
					hasRun = true;
				}
			}		
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Tokenizer create(AttributeFactory factory) {
		return new IKTokenizer(factory, useSmart());
	}
	

}
