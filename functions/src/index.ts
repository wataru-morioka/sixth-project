import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as moment from 'moment';
import { WriteBatch } from '@google-cloud/firestore';
admin.initializeApp(functions.config().firebase);
const db = admin.firestore();
const options = {
  priority: "high",
};

// 新規質問が登録された際、回答を問い合わせるターゲットのユーザを抽出し、DBに登録
exports.extractTargets = functions.region('asia-northeast1').firestore
  .document('questions/{questionId}')
  .onCreate(async (snap, context) => {
    const question = snap.data();
    if (question === undefined) {
      return 1;
    }
    console.log('新規質問受信：' + question.id);
    const users = db.collection('users');
    let count: number = 0;
    let targetNumber: number = 0;

    // ユーザ総数をカウント
    await users.get().then(allUsers => {
        count = allUsers.size;
      })
      .catch(err => {
          console.log('Error getting documents', err);
      });

    // ユーザ総数とお問い合わせ対象人数を比較
    if (count < question.targetNumber) {
      targetNumber = count;
    } else {
      targetNumber = question.targetNumber;
    }

    const targetArray: string[] = new Array();

    // 対象人数分、ランダムにユーザを抽出
    while (targetArray.length < targetNumber) {
      // 乱数ドキュメントID取得
      const key = users.doc().id;
      let exist = false;

      // 乱数より大きいドキュメントIDをもつユーザがいた場合、一人抽出
      await users
      .where(admin.firestore.FieldPath.documentId(), '>=', key)
      .limit(1)
      .get()
      .then(userDocs => {
        if(userDocs.size <= 0) { return; }
        exist = true;
        Promise.all(userDocs.docs.map(async user => {
          if (targetArray.indexOf(user.id) >= 0 || user.id === question.uid) { return; }
          console.log(user.id, '=>', user.data());
          targetArray.push(user.id);
          // targetコレクションに新規登録
          await addTargets(user.id, context.params.questionId, question.minutes);
        })
        ).then( _ => {
          console.log('登録完了');
        })
        .catch(err => {
          console.log('登録エラー', err);
        });
      })
      .catch(err => {
        console.log('Error getting documents', err);
      });
      
      if (exist) { continue; }

      // 乱数より小さいドキュメントIDをもつユーザがいた場合、一人抽出
      await users
      .where(admin.firestore.FieldPath.documentId(), '<=', key)
      .limit(1)
      .get()
      .then(userDocs => {
        Promise.all(userDocs.docs.map(async user => {
          if (targetArray.indexOf(user.id) >= 0 || user.id === question.uid) { return; }
          console.log(user.id, '=>', user.data());
          targetArray.push(user.id);
          // targetコレクションに新規登録
          await addTargets(user.id, context.params.questionId, question.minutes);
        })
        ).then( _ => {
          console.log('登録完了');
        })
        .catch(err => {
          console.log('登録エラー');
        });
      })
      .catch(err => {
        console.log('Error getting documents', err);
      });
    }
    return 0;
});

// 時間制限を過ぎた質問を抽出し、それぞれ非同期に集計し、更新すべきドキュメントを非同期に更新
exports.aggregate = functions.region('asia-northeast1').https.onRequest( async (request, response) => {
  setTimeout( () => {
    response.send('集計処理バッチ開始');
  }, 1)
  // let loopFlag = true;
  // while (loopFlag) {
    console.log('ループ');
    let result = true;

    //集計対象の質問を抽出
    let targetQuestionIdArray: string[] = new Array();
    await extractQuestion()
    .then(questions => {
      targetQuestionIdArray = questions;
    }).catch(err => {
      console.log(err);
      result = false;
    });

    //非同期で各質問の集計をし、（questions,targets,answers）コレクションを更新※トランザクション
    Promise.all(
      targetQuestionIdArray.map(async questionId => {
        //集計
        let answer1number = 0;
        let answer2number = 0;
        await Promise.all([
          aggregate(questionId, 1),
          aggregate(questionId, 2)
          ]
        ).then(results => {
          answer1number = results[0];
          answer2number = results[1];
        }).catch(err => {
          console.log(err);
          result = false;
        });

        console.log('トランザクション開始');
        const batch = db.batch();
        await Promise.all([
          updateTargets(batch, questionId),
          updateAnswers(batch, questionId),
          updateQuestion(batch, questionId, answer1number, answer2number)
        ])
        .then(async _ => {
          console.log('コミット開始');
          await batch.commit()
          .then(function () {
            console.log("トランザクション完了");
          })
          .catch(err => {
            console.log('コミットエラー', err);
            result = false;
          });
        })
        .catch(err => {
          console.log('集計処理エラー', err);
          result = false;
        })
     
        if (!result) {
          // loopFlag = result;
          return;
        }
      })
    ).then( _ => {
      console.log('集計バッチ完了');
    })
    .catch(err => {
      console.log('集計バッチエラー');
      // loopFlag = false;
    });

  //   await sleep(60);
  // }
});

const extractQuestion = () => {
  return new Promise<string[]>(async (resolve, reject) => {
    const now = moment().add(9, 'hour').format('YYYY-MM-DD HH:mm:ss');
    const targetQuestionIdArray: string[] = new Array();

    await db.collection('questions')
    .where('determinationFlag', '==', false)
    .where('timeLimit', '<', now)
    .get()
    .then(targetQuestions => {
      Promise.all(
        targetQuestions.docs.map(async targetQuestion => {
          console.log('抽出対象：' + targetQuestion.id);
          targetQuestionIdArray.push(targetQuestion.id);
          return;
        })
      ).then( _ => {
          console.log('集計対象抽出成功');
          resolve(targetQuestionIdArray);
        }
      ).catch(err => {
          console.log('集計対象抽出エラー');
          reject({});
        }
      );
    })
    .catch(err => {
      console.log('Error getting documents', err);
      reject({});
    });
  })
}

// 回答を集計
const aggregate = (questionId: string, decision: number) => {
  return new Promise<number>(async (resolve, reject) => {
    await db.collection('answers')
    .where('serverQuestionId', '==', questionId)
    .where('decision', '==', decision)
    .get()
    .then(results => {
      console.log(decision + ' questionId:' + questionId + ' ' + results.size);
      resolve(results.size);
    })
    .catch(err => {
      reject(-1);
    });
  }); 
}

// 集計が完了したことを回答者ドキュメントに更新
const updateTargets = (batch: WriteBatch, questionId: string) => {
  return new Promise<void>(async (resolve, reject) => {
    console.log('targets更新開始');
    await db.collection('targets')
    .where('serverQuestionId', '==', questionId)
    .get()
    .then(async targets => {
      await Promise.all(
        targets.docs.map(async target => {
          console.log(target.id);
          const targetRef = db.collection('targets').doc(target.id); 
          batch.update(targetRef, {
            'determinationFlag': true 
          });
          return;
        })
      ).then(_ => {
          console.log('targets更新完了');
          resolve();
        }
      )
      .catch(err => {
          console.log('targets更新エラー');
          reject();
        }
      );
    })
    .catch(err => {
      console.log('Error2 getting documents', err);
      reject();
    });
  })
}

// 集計が完了したことをanswerドキュメントに更新
const updateAnswers = (batch: WriteBatch, questionId: string) => {
  return new Promise<void>(async (resolve, reject) => {
    console.log('answers更新開始');
    await db.collection('answers')
    .where('serverQuestionId', '==', questionId)
    .get()
    .then(async answers =>{
      await Promise.all(
        answers.docs.map(async answer => {
          console.log(answer.id);
          const answerRef = db.collection('answers').doc(answer.id);
          batch.update(answerRef, {
            'determinationFlag': true
          });
          return;
        })
      ).then(_ => {
          console.log('answers更新完了');
          resolve();
        }
      ).catch(err => {
          console.log('answers更新エラー');
          reject();
        }
      );
    })
    .catch(err => {
      console.log('Error3 getting documents', err);
      reject();
    });
  })
}

// 集計結果をquestionドキュメントに更新
const updateQuestion = (batch: WriteBatch, questionId: string, answer1number: number, answer2number: number) => {
  return new Promise<void>((resolve, reject) => {
    console.log('questions更新開始');
    const questionRef = db.collection('questions').doc(questionId);
    batch.update(questionRef, {
      'answer1number': answer1number,
      'answer2number': answer2number,
      'determinationFlag': true
    });
    resolve();
  })
}

// 新規質問の回答者として抽出された対象回答者に対し、プッシュ通知
exports.pushAskingToTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onCreate(async (snap, context) => {
    const target = snap.data();
    if (target === undefined) {
      return 1;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '新着質問',
        body: '新しい質問を受信しました',
        // badge: "1",
        sound:"default",
      }
    };

    await notify(payload, target['uid']);

    // プッシュ通知が完了した情報をtargetドキュメントに更新
    await db.collection('targets').doc(context.params.targetId).update({
      'askPushFlag': true
    });
    
    return 0;
});

// 集計が完了した質問の回答者に対し、プッシュ通知
exports.pushResultTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onUpdate( async (snap, context) => {
    const target = snap.after.data();
    if (target === undefined) {
      return 1;
    }

    if (!target['determinationFlag'] || target['resultReceiveFlag'] || target['finalPushFlag']) {
      return 0;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '集計結果受信',
        body: '他人の質問の集計が完了しました',
        // badge: "1",
        sound:"default",
      },
    };

    await notify(payload, target['uid']);

    // プッシュ通知が完了した情報をtargetドキュメントに更新
    await db.collection('targets').doc(context.params.targetId).update({
      'finalPushFlag': true
    });
    
    return 0;
});

// 集計が完了した質問の質問者に対し、プッシュ通知
exports.pushResultToOwners = functions.region('asia-northeast1').firestore
  .document('questions/{questionId}')
  .onUpdate( async (snap, context) => {
    const question = snap.after.data();
    if (question === undefined) {
      return 1;
    }

    if (!question['determinationFlag'] || question['resultReceiveFlag'] || question['finalPushFlag']) {
      return 0;
    }

    //プッシュ通知
    const payload = {
      notification: {
        title: '集計結果受信',
        body: '自分の質問の集計が完了しました',
        // badge: "1",
        sound:"default",
      }
    };

    await notify(payload, question['uid']);

    // プッシュ通知が完了した情報を質問ドキュメントに更新
    await db.collection('questions').doc(context.params.questionId).update({
      'finalPushFlag': true
    });
    
    return 0;
});

// 集計結果を受信したtarget（質問に対し回答をしたユーザ）ドキュメントを削除
exports.deleteTargets = functions.region('asia-northeast1').firestore
  .document('targets/{targetId}')
  .onUpdate( async (snap, context) => {
    const target = snap.after.data();
    if (target === undefined) {
      return 1;
    }

    if (target['resultReceiveFlag'] && target['finalPushFlag']) {
      console.log('削除target:'+ context.params.targetId);
      await db.collection('targets').doc(context.params.targetId).delete();
    }
    
    return 0;
});

// 回答の集計が完了した質問に対する回答ドキュメントを削除
exports.deleteAnswers = functions.region('asia-northeast1').firestore
  .document('answers/{answerId}')
  .onUpdate( async (snap, context) => {
    const answer = snap.after.data();
    if (answer === undefined) {
      return 1;
    }

    if (answer['determinationFlag']) {
      console.log('削除target:'+ context.params.answerId);  
      await db.collection('answers').doc(context.params.answerId).delete();
    }
    
    return 0;
});

// const sleep = (waitSeconds: number) => {
//   return new Promise(resolve => {
//       setTimeout(() => {
//         resolve();
//     }, waitSeconds * 1000);
//   })
// }

// お問い合わせ対象のユーザドキュメントをtargetコレクションに新規登録
async function addTargets(uid: string, questionId: string, minutes: number) {
  const now = moment().add(9, 'hour').format('YYYY-MM-DD HH:mm:ss');
  const timeLimit = moment().add(9, 'hour').add(minutes, 'minute').format('YYYY-MM-DD HH:mm:ss');

  const targetRef = admin.firestore().collection('targets').doc(`${uid}_${questionId}`);
  const questionRef = db.collection('questions').doc(questionId);
  
  const batch = db.batch();

  // targetコレクションに新規登録
  batch.set(targetRef, {
    'uid': uid,
    'serverQuestionId': questionId,
    'timeLimit': timeLimit,
    'askPushFlag': false,
    'askReceiveFlag': false,
    'answerFlag': false,
    'determinationFlag': false,
    'finalPushFlag': false,
    'resultReceiveFlag': false,
    'createdDateTime': now,
    'modifiedDateTime': null,
  });

  // questionコレクションを更新
  batch.update(questionRef, {
    'timeLimit': timeLimit,
    'askFlag': true
  });

  await batch.commit()
  .then(function () {
    console.log("トランザクション完了");
  })
  .catch(err => {
    console.log('Error getting documents', err);
  });
}

// 対象ユーザにプッシュ通知を送信
async function notify(payload: {}, uid: string) {
  let token: string = ''
  let result = false;

  // ユーザのidトークン取得
  await db.collection('users').doc(uid).get().then(user => {
    const userInfo = user.data()
    if (userInfo === undefined) {
      return;
    }
    token = userInfo.token;
    if (token.length > 0) {
      console.log(token);
      result = true;
    }
  })

  if (!result) {
    return;
  }

  // 対象idトークンをもつユーザに対し、プッシュ通知
  admin.messaging().sendToDevice(token, payload, options)
  .then(pushResponse => {
    console.log("Successfully sent message:", pushResponse);
  })
  .catch(error => {
    console.log("Error sending message:", error);
  });
}